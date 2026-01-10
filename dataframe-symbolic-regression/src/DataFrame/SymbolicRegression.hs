{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.SymbolicRegression where

import Control.Exception (throw)
import Control.Monad.State.Strict
import Data.Massiv.Array as MA hiding (forM, forM_)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Internal.Expression
import System.Random

import Algorithm.EqSat.Build
import Algorithm.EqSat.DB
import Algorithm.EqSat.Egraph
import Algorithm.EqSat.Info
import Algorithm.EqSat.Queries
import Algorithm.EqSat.Simplify hiding (myCost)
import Algorithm.SRTree.Likelihoods
import Algorithm.SRTree.ModelSelection (fractionalBayesFactor)
import Control.Lens (over)
import Control.Monad (
    filterM,
    forM,
    forM_,
    replicateM,
    unless,
    when,
    (>=>),
 )
import Data.Binary (decode, encode)
import qualified Data.ByteString.Lazy as BS
import Data.Function (on)
import Data.Functor
import qualified Data.HashSet as Set
import qualified Data.IntMap.Strict as IM
import Data.List (
    intercalate,
    maximumBy,
    nub,
    zip4,
 )
import Data.List.Split (splitOn)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromJust)
import Data.SRTree
import Data.SRTree.Datasets
import qualified Data.SRTree.Internal as SI
import Data.SRTree.Print
import Data.SRTree.Random

import Algorithm.EqSat (runEqSat)
import Algorithm.EqSat.SearchSR
import Data.Time.Clock.POSIX
import Text.ParseSR

data RegressionConfig = RegressionConfig
    { generations :: Int
    , maxExpressionSize :: Int
    , numFolds :: Int
    , showTrace :: Bool
    , lossFunction :: Distribution
    , numOptimisationIterations :: Int
    , numParameterRetries :: Int
    , populationSize :: Int
    , tournamentSize :: Int
    , crossoverProbability :: Double
    , mutationProbability :: Double
    , unaryFunctions :: [D.Expr Double -> D.Expr Double]
    , binaryFunctions :: [D.Expr Double -> D.Expr Double -> D.Expr Double]
    , numParams :: Int -- -1 for auto
    , generational :: Bool
    , simplifyExpressions :: Bool
    , maxTime :: Int -- -1 for no limit
    , dumpTo :: String
    , loadFrom :: String
    }

defaultRegressionConfig :: RegressionConfig
defaultRegressionConfig =
    RegressionConfig
        { generations = 100
        , maxExpressionSize = 5
        , numFolds = 3
        , showTrace = True
        , lossFunction = MSE
        , numOptimisationIterations = 30
        , numParameterRetries = 2
        , populationSize = 100
        , tournamentSize = 3
        , crossoverProbability = 0.95
        , mutationProbability = 0.3
        , unaryFunctions = []
        , binaryFunctions = [(+), (-), (*), (/)]
        , numParams = -1
        , generational = False
        , simplifyExpressions = True
        , maxTime = -1
        , dumpTo = ""
        , loadFrom = ""
        }

fitSymbolicRegression ::
    RegressionConfig -> D.Expr Double -> D.DataFrame -> IO [D.Expr Double]
fitSymbolicRegression cfg targetColumn df = do
    g <- getStdGen
    let
        df' =
            D.exclude
                [F.name targetColumn]
                (D.selectBy [D.byProperty (D.hasElemType @Double)] df)
        matrix = either throw id (D.toDoubleMatrix df')
        features = fromLists' Seq (V.toList (V.map VU.toList matrix)) :: Array S Ix2 Double
        target' = fromLists' Seq (D.columnAsList targetColumn df) :: Array S Ix1 Double
        nonterminals =
            intercalate
                ","
                ( Prelude.map
                    (toNonTerminal . (\f -> f (F.col "fake1") (F.col "fake2")))
                    (binaryFunctions cfg)
                )
        varnames =
            intercalate
                ","
                ( Prelude.map
                    T.unpack
                    (Prelude.filter (/= F.name targetColumn) (D.columnNames df))
                )
        alg =
            evalStateT
                ( egraphGP
                    cfg
                    nonterminals
                    varnames
                    [((features, target', Nothing), (features, target', Nothing))]
                    [(features, target', Nothing)]
                )
                emptyGraph
    fmap (Prelude.map (toExpr df')) (evalStateT alg g)

toExpr :: D.DataFrame -> Fix SRTree -> Expr Double
toExpr _ (Fix (Const value)) = Lit value
toExpr df (Fix (Var ix)) = Col (D.columnNames df !! ix)
toExpr df (Fix (Bin op left right)) = case op of
    SI.Add -> toExpr df left + toExpr df right
    SI.Sub -> toExpr df left - toExpr df right
    SI.Mul -> toExpr df left * toExpr df right
    SI.Div -> toExpr df left / toExpr df right
    _ -> error "UNIMPLEMENTED"
toExpr _ _ = error "UNIMPLEMENTED"

toNonTerminal :: D.Expr Double -> String
toNonTerminal (BinaryOp "add" _ _ _) = "add"
toNonTerminal (BinaryOp "sub" _ _ _) = "sub"
toNonTerminal (BinaryOp "mult" _ _ _) = "mul"
toNonTerminal (BinaryOp "divide" _ _ _) = "div"
toNonTerminal e = error ("Unsupported operation: " ++ show e)

egraphGP ::
    RegressionConfig ->
    String -> -- nonterminals
    String -> -- varnames
    [(DataSet, DataSet)] ->
    [DataSet] ->
    StateT EGraph (StateT StdGen IO) [Fix SRTree]
egraphGP cfg nonterminals varnames dataTrainVals dataTests = do
    unless (null (loadFrom cfg)) $
        io (BS.readFile (loadFrom cfg)) >>= \eg -> put (decode eg)

    _ <- insertTerms
    evaluateUnevaluated fitFun

    t0 <- io getPOSIXTime

    pop <- replicateM (populationSize cfg) $ do
        ec <- insertRndExpr (maxExpressionSize cfg) rndTerm rndNonTerm >>= canonical
        _ <- updateIfNothing fitFun ec
        pure ec
    pop' <- Prelude.mapM canonical pop

    output <-
        if showTrace cfg
            then forM (Prelude.zip [0 ..] pop') $ uncurry printExpr'
            else pure []

    let mTime =
            if maxTime cfg < 0 then Nothing else Just (fromIntegral $ maxTime cfg - 5)
    (_, _, _) <- iterateFor (generations cfg) t0 mTime (pop', output, populationSize cfg) $ \_ (ps', out, curIx) -> do
        newPop' <- replicateM (populationSize cfg) (evolve ps')

        out' <-
            if showTrace cfg
                then forM (Prelude.zip [curIx ..] newPop') $ uncurry printExpr'
                else pure []

        totSz <- gets (Map.size . _eNodeToEClass)
        let full = totSz > max maxMem (populationSize cfg)
        when full (cleanEGraph >> cleanDB)

        newPop <-
            if generational cfg
                then Prelude.mapM canonical newPop'
                else do
                    pareto <-
                        concat <$> forM [1 .. maxExpressionSize cfg] (`getTopFitEClassWithSize` 2)
                    let remainder = populationSize cfg - length pareto
                    lft <-
                        if full
                            then getTopFitEClassThat remainder (const True)
                            else pure $ Prelude.take remainder newPop'
                    Prelude.mapM canonical (pareto <> lft)
        pure (newPop, out <> out', curIx + populationSize cfg)

    unless (null (dumpTo cfg)) $
        get >>= (io . BS.writeFile (dumpTo cfg) . encode)
    paretoFront' fitFun (maxExpressionSize cfg)
  where
    maxMem = 2000000
    fitFun =
        fitnessMV
            shouldReparam
            (numParameterRetries cfg)
            (numOptimisationIterations cfg)
            (lossFunction cfg)
            dataTrainVals
    nonTerms = parseNonTerms nonterminals
    (Sz2 _ nFeats) = case dataTrainVals of
        [] -> Sz2 0 0
        (h : _) -> MA.size (getX . fst $ h)
    params =
        if numParams cfg == -1
            then [param 0]
            else Prelude.map param [0 .. numParams cfg - 1]
    shouldReparam = numParams cfg == -1
    relabel = if shouldReparam then relabelParams else relabelParamsOrder
    terms =
        if lossFunction cfg == ROXY
            then var 0 : params
            else [var ix | ix <- [0 .. nFeats - 1]]
    uniNonTerms = [t | t <- nonTerms, isUni t]
    binNonTerms = [t | t <- nonTerms, isBin t]

    isUni (Uni _ _) = True
    isUni _ = False

    isBin (Bin{}) = True
    isBin _ = False

    cleanEGraph = do
        let nParetos = 10
        io . putStrLn $ "cleaning"
        pareto <-
            forM [1 .. maxExpressionSize cfg] (`getTopFitEClassWithSize` nParetos)
                >>= Prelude.mapM canonical . concat
        infos <- forM pareto (\c -> gets (_info . (IM.! c) . _eClass))
        exprs <- forM pareto getBestExpr
        put emptyGraph
        newIds <- fromTrees myCost $ Prelude.map relabel exprs
        forM_ (Prelude.zip newIds (Prelude.reverse infos)) $ \(eId, info') ->
            insertFitness eId (fromJust $ _fitness info') (_theta info')

    rndTerm = do
        coin <- toss
        if coin || numParams cfg == 0 then randomFrom terms else randomFrom params

    rndNonTerm = randomFrom nonTerms

    refitChanged = do
        ids <-
            (gets (_refits . _eDB) >>= Prelude.mapM canonical . Set.toList)
                Data.Functor.<&> nub
        modify' $ over (eDB . refits) (const Set.empty)
        forM_ ids $ \ec -> do
            t <- getBestExpr ec
            (f, p) <- fitFun t
            insertFitness ec f p

    iterateFor 0 _ _ xs _ = pure xs
    iterateFor n t0' maxT xs f = do
        xs' <- f n xs
        t1 <- io getPOSIXTime
        let delta = t1 - t0'
            maxT' = subtract delta <$> maxT
        case maxT' of
            Nothing -> iterateFor (n - 1) t1 maxT' xs' f
            Just mt ->
                if mt <= 0
                    then pure xs
                    else iterateFor (n - 1) t1 maxT' xs' f

    evolve xs' = do
        xs <- Prelude.mapM canonical xs'
        parents' <- tournament xs
        offspring <- combine parents'
        if numParams cfg == 0
            then runEqSat myCost rewritesWithConstant 1 >> cleanDB >> refitChanged
            else runEqSat myCost rewritesParams 1 >> cleanDB >> refitChanged
        canonical offspring >>= updateIfNothing fitFun >> pure ()
        canonical offspring

    tournament xs = do
        p1 <- applyTournament xs >>= canonical
        p2 <- applyTournament xs >>= canonical
        pure (p1, p2)

    applyTournament :: [EClassId] -> RndEGraph EClassId
    applyTournament xs = do
        challengers <-
            replicateM (tournamentSize cfg) (rnd $ randomFrom xs) >>= traverse canonical
        fits <- Prelude.map fromJust <$> Prelude.mapM getFitness challengers
        pure . snd . maximumBy (compare `on` fst) $ Prelude.zip fits challengers

    combine (p1, p2) = crossover p1 p2 >>= mutate >>= canonical

    crossover p1 p2 = do
        sz <- getSize p1
        coin <- rnd $ tossBiased (crossoverProbability cfg)
        if sz == 1 || not coin
            then rnd (randomFrom [p1, p2])
            else do
                pos <- rnd $ randomRange (1, sz - 1)
                cands <- getAllSubClasses p2
                tree <- getSubtree pos 0 Nothing [] cands p1
                fromTree myCost (relabel tree) >>= canonical

    getSubtree ::
        Int ->
        Int ->
        Maybe (EClassId -> ENode) ->
        [Maybe (EClassId -> ENode)] ->
        [EClassId] ->
        EClassId ->
        RndEGraph (Fix SRTree)
    getSubtree 0 sz (Just parent) mGrandParents cands p' = do
        p <- canonical p'
        candidates' <-
            filterM (fmap (< maxExpressionSize cfg - sz) . getSize) cands
        candidates <-
            filterM (doesNotExistGens mGrandParents . parent) candidates'
                >>= traverse canonical
        if null candidates
            then getBestExpr p
            else do
                subtree <- rnd (randomFrom candidates)
                getBestExpr subtree
    getSubtree pos sz parent mGrandParents cands p' = do
        p <- canonical p'
        root <- getBestENode p >>= canonize
        case root of
            Param ix -> pure . Fix $ Param ix
            Const x -> pure . Fix $ Const x
            Var ix -> pure . Fix $ Var ix
            Uni f t' -> do
                t <- canonical t'
                Fix . Uni f
                    <$> getSubtree (pos - 1) (sz + 1) (Just $ Uni f) (parent : mGrandParents) cands t
            Bin op l'' r'' -> do
                l <- canonical l''
                r <- canonical r''
                szLft <- getSize l
                szRgt <- getSize r
                if szLft < pos
                    then do
                        l' <- getBestExpr l
                        r' <-
                            getSubtree
                                (pos - szLft - 1)
                                (sz + szLft + 1)
                                (Just $ Bin op l)
                                (parent : mGrandParents)
                                cands
                                r
                        pure . Fix $ Bin op l' r'
                    else do
                        l' <-
                            getSubtree
                                (pos - 1)
                                (sz + szRgt + 1)
                                (Just (\t -> Bin op t r))
                                (parent : mGrandParents)
                                cands
                                l
                        r' <- getBestExpr r
                        pure . Fix $ Bin op l' r'

    getAllSubClasses p' = do
        p <- canonical p'
        en <- getBestENode p
        case en of
            Bin _ l r -> do
                ls <- getAllSubClasses l
                rs <- getAllSubClasses r
                pure (p : ls <> rs)
            Uni _ t -> (p :) <$> getAllSubClasses t
            _ -> pure [p]

    mutate p = do
        sz <- getSize p
        coin <- rnd $ tossBiased (mutationProbability cfg)
        if coin
            then do
                pos <- rnd $ randomRange (0, sz - 1)
                tree <- mutAt pos (maxExpressionSize cfg) Nothing p
                fromTree myCost (relabel tree) >>= canonical
            else pure p

    peel :: Fix SRTree -> SRTree ()
    peel (Fix (Bin op _ _)) = Bin op () ()
    peel (Fix (Uni f _)) = Uni f ()
    peel (Fix (Param ix)) = Param ix
    peel (Fix (Var ix)) = Var ix
    peel (Fix (Const x)) = Const x

    mutAt ::
        Int -> Int -> Maybe (EClassId -> ENode) -> EClassId -> RndEGraph (Fix SRTree)
    mutAt 0 sizeLeft Nothing _ = insertRndExpr sizeLeft rndTerm rndNonTerm >>= canonical >>= getBestExpr
    mutAt 0 1 _ _ = rnd $ randomFrom terms
    mutAt 0 sizeLeft (Just parent) _ = do
        ec <- insertRndExpr sizeLeft rndTerm rndNonTerm >>= canonical
        (Fix tree) <- getBestExpr ec
        root <- getBestENode ec
        exist <- canonize (parent ec) >>= doesExist
        if exist
            then do
                let children = childrenOf root
                candidates <- case length children of
                    0 ->
                        filterM
                            (checkToken parent . replaceChildren children)
                            (Prelude.map peel terms)
                    1 -> filterM (checkToken parent . replaceChildren children) uniNonTerms
                    2 -> filterM (checkToken parent . replaceChildren children) binNonTerms
                    _ -> pure []
                if null candidates
                    then pure $ Fix tree
                    else do
                        newToken <- rnd (randomFrom candidates)
                        pure . Fix $ replaceChildren (childrenOf tree) newToken
            else pure . Fix $ tree
    mutAt pos sizeLeft _ p' = do
        p <- canonical p'
        root <- getBestENode p >>= canonize
        case root of
            Param ix -> pure . Fix $ Param ix
            Const x -> pure . Fix $ Const x
            Var ix -> pure . Fix $ Var ix
            Uni f t' ->
                canonical t'
                    >>= ( fmap (Fix . Uni f)
                            . mutAt (pos - 1) (sizeLeft - 1) (Just $ Uni f)
                        )
            Bin op ln rn -> do
                l <- canonical ln
                r <- canonical rn
                szLft <- getSize l
                szRgt <- getSize r
                if szLft < pos
                    then do
                        l' <- getBestExpr l
                        r' <- mutAt (pos - szLft - 1) (sizeLeft - szLft - 1) (Just $ Bin op l) r
                        pure . Fix $ Bin op l' r'
                    else do
                        l' <- mutAt (pos - 1) (sizeLeft - szRgt - 1) (Just (\t -> Bin op t r)) l
                        r' <- getBestExpr r
                        pure . Fix $ Bin op l' r'

    printExpr' :: Int -> EClassId -> RndEGraph [String]
    printExpr' ix ec = do
        thetas' <- gets (_theta . _info . (IM.! ec) . _eClass)
        bestExpr <-
            (if simplifyExpressions cfg then simplifyEqSatDefault else id)
                <$> getBestExpr ec

        let best' =
                if shouldReparam then relabelParams bestExpr else relabelParamsOrder bestExpr
            nParams' = countParamsUniq best'
            fromSz (MA.Sz x) = x
            nThetas = Prelude.map (fromSz . MA.size) thetas'
        (_, thetas) <-
            if Prelude.any (/= nParams') nThetas
                then fitFun best'
                else pure (1.0, thetas')

        maxLoss <- negate . fromJust <$> getFitness ec
        forM (Data.List.zip4 [(0 :: Int) ..] dataTrainVals dataTests thetas) $ \(view, (dataTrain, dataVal), dataTest, theta') -> do
            let (x, y, mYErr) = dataTrain
                (x_val, y_val, mYErr_val) = dataVal
                (x_te, y_te, mYErr_te) = dataTest
                distribution = lossFunction cfg

                expr = paramsToConst (MA.toList theta') best'
                showNA z = if isNaN z then "" else show z
                r2_train = r2 x y best' theta'
                r2_val = r2 x_val y_val best' theta'
                r2_te = r2 x_te y_te best' theta'
                nll_train = nll distribution mYErr x y best' theta'
                nll_val = nll distribution mYErr_val x_val y_val best' theta'
                nll_te = nll distribution mYErr_te x_te y_te best' theta'
                mdl_train = fractionalBayesFactor distribution mYErr x y theta' best'
                mdl_val = fractionalBayesFactor distribution mYErr_val x_val y_val theta' best'
                mdl_te = fractionalBayesFactor distribution mYErr_te x_te y_te theta' best'
                vals =
                    intercalate "," $
                        Prelude.map
                            showNA
                            [ nll_train
                            , nll_val
                            , nll_te
                            , maxLoss
                            , r2_train
                            , r2_val
                            , r2_te
                            , mdl_train
                            , mdl_val
                            , mdl_te
                            ]
                thetaStr = intercalate ";" $ Prelude.map show (MA.toList theta')
                showExprFun = if null varnames then showExpr else showExprWithVars (splitOn "," varnames)
                showLatexFun = if null varnames then showLatex else showLatexWithVars (splitOn "," varnames)
            pure $
                show ix
                    <> ","
                    <> show view
                    <> ","
                    <> showExprFun expr
                    <> ","
                    <> "\""
                    <> showPython best'
                    <> "\","
                    <> "\"$$"
                    <> showLatexFun best'
                    <> "$$\","
                    <> thetaStr
                    <> ","
                    <> show @Int (countNodes $ convertProtectedOps expr)
                    <> ","
                    <> vals

    insertTerms = forM terms (fromTree myCost >=> canonical)

    paretoFront' _ maxSize' = go 1 (-(1.0 / 0.0))
      where
        go :: Int -> Double -> RndEGraph [Fix SRTree]
        go n f
            | n > maxSize' = pure []
            | otherwise = do
                ecList <- getBestExprWithSize n
                if not (null ecList)
                    then do
                        let (ec', mf) = case ecList of
                                [] -> (0, Nothing)
                                (e : _) -> e
                            f' = fromJust mf
                            improved = f' >= f && not (isNaN f') && not (isInfinite f')
                        ec <- canonical ec'
                        if improved
                            then do
                                thetas' <- gets (_theta . _info . (IM.! ec) . _eClass)
                                bestExpr <-
                                    relabelParams . (if simplifyExpressions cfg then simplifyEqSatDefault else id)
                                        <$> getBestExpr ec
                                let t = case thetas' of
                                        [] -> Fix (Const 0) -- Not sure if this makes sense as a default.
                                        (h : _) -> paramsToConst (MA.toList h) bestExpr
                                ts <- go (n + 1) (max f f')
                                pure (t : ts)
                            else go (n + 1) (max f f')
                    else go (n + 1) f
