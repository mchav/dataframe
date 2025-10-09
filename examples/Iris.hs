{-# LANGUAGE  
        DeriveGeneric
    ,   MultiParamTypeClasses 
    ,   OverloadedStrings
    ,   RecordWildCards
    ,   TypeApplications 
#-}

module Main where

import           GHC.Generics         ( Generic )

import           Control.Monad        ( when )
import           Data.List            ( maximumBy )
import           Data.Function        ( on )

import qualified Data.Text                     as T 
import qualified Data.Vector                   as V
import qualified Data.Vector.Unboxed           as VU

import           DataFrame            ( (|>) ) 
import qualified DataFrame                     as D
import qualified DataFrame.Functions           as F
import qualified DataFrame.Hasktorch           as DHT


import qualified Torch                         as HT

import qualified System.Random                 as SysRand



-- In this example, we reproduce the tutorial given in 
-- https://machinelearningmastery.com/building-a-multiclass-classification-model-in-pytorch/
-- entirely in Haskell, with DataFrame and Hasktorch.

main :: IO ()
main = do
    -- There are five columns in the iris dataset
    -- All columns are of type Double, except for "variety" 
    -- which is of type Text.
    df <- D.readParquet "../data/iris.parquet"

    -- We can leverage Haskell's rich type system to represent
    -- categorical data. In this case we are able to use `read :: Iris` 
    -- to convert a String to an Int directly (see below)
    -- Later, we can call toEnum to get back an instance of Iris.
    let derivedDf = df 
          |> D.derive 
             "variety"
             (F.lift
                (\txt -> fromEnum (read (T.unpack txt) :: Iris)) 
                (F.col @T.Text "variety"))
            
    -- DataFrame's `randomSplit` functions much like Scikit-Learn's `train_test_split`
    -- Allowing us to obtain mutually exclusive training and test sets.
    -- You can choose your own seed using `mkStdGen`.
    let (trainDf, testDf) = D.randomSplit (SysRand.mkStdGen 42) 0.7 derivedDf

    -- DataFrame's `select` function allows us to choose our predictor values
    -- in a manner similar to R's dplyr.
    -- We can chain computations together by forwarding them with the (|>) operator.
    -- `toTensor` is exported by DataFrame.Hasktorch as a utility for obtaining a tensor
    -- that can directly be used by Hasktorch. The tensor has  the same number of 
    -- rows and columns as the underlying dataframe.
    let trainFeaturesTr = trainDf 
            |> D.select ["sepal.length", "sepal.width", "petal.length", "petal.width"] 
            |> DHT.toTensor
    let testFeaturesTr  = testDf
            |> D.select ["sepal.length", "sepal.width", "petal.length", "petal.width"] 
            |> DHT.toTensor

    let trainLabels = D.columnAsVector @Int "variety" trainDf
    let testLabels  = D.columnAsVector @Int "variety" testDf
    
    -- We have to one-hot encode our training data. Each item in our training data is 
    -- mapped to one of 
    -- [1.0, 0.0, 0.0] -> Setosa
    -- [0.0, 1.0, 0.0] -> Versicolor
    -- [0.0, 0.0, 1.0] -> Virginica
    let trainLabelsTr   = HT.toType HT.Float $ HT.oneHot 3 $ HT.asTensor $ V.toList trainLabels
    let testLabelsTr    = HT.toType HT.Float $ HT.oneHot 3 $ HT.asTensor $ V.toList testLabels
    
    -- We have made MLP an instance of Randomizable, so we can simply use HT.sample to 
    -- get an initial neural network with randomly assigned weights and biases.
    initialModel <- HT.sample $ MLPSpec 4 8 3 -- 4 input feature columns, 8 hidden nodes in the hidden layer, 3 labels in the output

    -- We can now run the training loop. Refer below for more details.
    trainedModel <- trainLoop 
        10000
        (trainFeaturesTr, trainLabelsTr)
        (testFeaturesTr,  testLabelsTr )
        initialModel 
    
    putStrLn "Your model weights are given as follows: "
    print trainedModel

    putStrLn "-----------------------------------------"
    putStrLn "Training Set Predictions are are as follows: "
    let predTrain = reverseOneHot $ mlp trainedModel trainFeaturesTr
    mapM_ (\(a, b) -> putStrLn $ "Actual label: " ++ show (toEnum a :: Iris)  ++ ", Predicted label: " ++ show (toEnum b :: Iris)) (zip (V.toList trainLabels) predTrain)


    putStrLn "-----------------------------------------"   
    putStrLn "Test Set Predictions are are as follows: "
    let predTest = reverseOneHot $ mlp trainedModel testFeaturesTr
    mapM_ (\(a, b) -> putStrLn $ "Actual label: " ++ show (toEnum a :: Iris)  ++ ", Predicted label: " ++ show (toEnum b :: Iris)) (zip (V.toList testLabels) predTest)







--  There are three types of Iris in our dataset.
--  We can use Haskell's type system to represent this, 
--  with an Algebraic data type, specifically the "sum" type.
--  (https://wiki.haskell.org/index.php?title=Algebraic_data_type)

--  Note :: We need Iris to be an instance of Ord
--  in order to use it in conjunction with D.derive.
--  But I don't quite understand why...
--  `No instance for ‘Ord Iris’ arising from a use of ‘D.derive’`

--  By deriving the Read typeclass, we are able to convert
--  the String to the appropriate type, e.g. "Setosa" -> Setosa, 
--  by using read x :: Iris.

--  By deriving the Enum typeclass, we are able to convert Iris
--  to an Int by using fromEnum x, and from Int to Iris using toEnum x. 
--  In this case, Setosa -> 0, Versicolor -> 1, Virginica -> 2
data Iris = Setosa
          | Versicolor
          | Virginica
    deriving (Eq, Show, Read, Ord, Enum) 


-- Reverses the one hot encoding. For example,
-- Input                    Output
-- [ [0.8, 0.1, 0.1]        [ 0      -- the 0th element has the highest probability
-- , [0.1, 0.7, 0.2]        , 1      -- the 1st element has the highest probability
-- ]                        ]
reverseOneHot :: HT.Tensor -> [Int]
reverseOneHot tsr = map (fst . maximumBy (compare `on` snd) . zip [0..]) vals
    where vals = HT.asValue tsr :: [[Float]]


--  Here, we have an implementation for a feedforward neural-network with fully
--  connected neurons, also known as a multi-level perceptron (MLP) 
--  This has one hidden layer.

--  [The next few sections are similar to the `__init__()` function 
--  when defining an instance of instance of PyTorch's `Module`.] 
--  MLPSpec defines how many layers there are. Eventually, we must set
--  `inputFeatures` to be 4, as there 4 features that we are using, and
--  `outputFeatures` to be 3, as there are 3 classes that we need to choose from. 
data MLPSpec = MLPSpec 
    { inputFeatures  :: Int
    , hiddenFeatures :: Int
    , outputFeatures :: Int
    } deriving (Show, Eq)


--  Now we are ready to define the datatype that represents our model.
--  `Linear` means that each node in the layer applies a linear transformation
--  to its input before forwarding to the next layer.
data MLP = MLP
    { l0 :: HT.Linear
    , l1 :: HT.Linear
    }
    deriving (Generic, Show)

-- We need to make MLPSpec an instance of randomizable,
-- so we can instantiate a completely random state from which to perform
-- gradient descent.
instance HT.Parameterized MLP
instance HT.Randomizable MLPSpec MLP where
    sample MLPSpec {..} = 
        MLP 
          <$> HT.sample (HT.LinearSpec inputFeatures  hiddenFeatures)
          <*> HT.sample (HT.LinearSpec hiddenFeatures outputFeatures)

-- [This section is similar to the `forward()` function when defining
-- an instance of PyTorch's `Module`.]
-- We must now specify the feed-forward flow of computation.
-- Note that in Hasktorch, this is function application, so we go from
-- (f_output . f_hidden . f_input), in that order.
mlp :: MLP -> HT.Tensor -> HT.Tensor
mlp MLP {..} = 
    HT.softmax (HT.Dim 1)
      . HT.linear l1
      . HT.relu           -- we mu
      . HT.linear l0


-- [This section is similar to the `for epoch in range(n_epochs)` training loop
--  for PyTorch.]
-- We can now define the training loop. We'll read in all of our training data
-- n times. Each time, we compute the `binaryCrossEntropyLoss'` and then 
-- use gradient descent (`GD`) to update the weights in our dataset.
-- Other optimizers include `Adam`, `Adagrad` and `GDM`.
-- You can modify this loop to break out early under other conditions, for 
-- example, if the testLoss increases.
trainLoop :: Int                     -- number of training epochs.
          -> (HT.Tensor, HT.Tensor)  -- training features, training labels
          -> (HT.Tensor, HT.Tensor)  -- test features, test labels
          -> MLP                     -- initial model with randomized weights
          -> IO MLP
trainLoop n 
          (features, labels) 
          (testFeatures, testLabels)
          initialM 
    = HT.foldLoop initialM n $ \state i -> do
        let predicted = mlp state features
        let loss      = HT.binaryCrossEntropyLoss' labels predicted
        when (i `mod` 100 == 0) $ do 
            let testPredicted = mlp state testFeatures
            let testLoss = HT.binaryCrossEntropyLoss' testLabels testPredicted
            putStrLn $ "Iteration :" ++ show i ++ " | Training Set Loss: " ++ show (HT.asValue loss :: Float) ++ " | Test Set Loss: " ++ show (HT.asValue testLoss :: Float)
        (state', _) <- HT.runStep state HT.GD loss 1e-2
        pure state'