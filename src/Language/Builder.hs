module Language.Builder
( with
, (=:)
) where

import Data.DataFrame
import Language.Babel
import Language.Kevin
import Control.Monad.Writer

with :: DataFrame -> Writer Kevin () -> IO Babel
with df kv = return $ genBabel df $ execWriter kv

(=:) = ($)

