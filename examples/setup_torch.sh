#!/usr/bin/env bash

set -xe
ghc --version

case "$(uname)" in
  "Darwin")
      TOTAL_MEM_GB=$(sysctl hw.memsize | awk '{print int($2/1024/1024/1024)}')
      NUM_CPU=$(sysctl -n hw.ncpu)
      GHC_OPTIONS="-optl-ld_classic"
    ;;
  "Linux")
      TOTAL_MEM_GB=$(grep MemTotal /proc/meminfo | awk '{print int($2/1024/1024)}')
      NUM_CPU=$(nproc --all)
      GHC_OPTIONS=""
    ;;
esac

USED_MEM_GB=$(echo "$TOTAL_MEM_GB" | awk '{print int(($1 + 1) / 2)}')
USED_NUM_CPU=$(echo "$NUM_CPU" | awk '{print int(($1 + 1) / 2)}')
USED_NUM_CPU=$(echo "$USED_MEM_GB" "$USED_NUM_CPU" | awk '{if($1<$2) {print $1} else {print $2}}')
USED_MEM_GB=$(echo "$USED_NUM_CPU" | awk '{print ($1)"G"}')
USED_MEMX2_GB=$(echo "$USED_NUM_CPU" | awk '{print ($1 * 2)"G"}')


cat <<EOF > cabal.project.local
package *
  shared: True
  executable-dynamic: True

package libtorch-ffi
    ghc-options: ${GHC_OPTIONS} -j${USED_NUM_CPU} +RTS -A128m -n2m -M${USED_MEM_GB} -RTS

package hasktorch
    ghc-options: -j${USED_NUM_CPU} +RTS -A128m -n2m -M${USED_MEMX2_GB} -RTS

package vector
    ghc-options: -j${USED_NUM_CPU} +RTS -A128m -n2m -M${USED_MEMX2_GB} -RTS

EOF
