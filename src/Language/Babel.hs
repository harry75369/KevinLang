module Language.Babel
( Babel(..)
, genBabel
) where

import Data.DataFrame
import Language.Kevin

data Babel = Babel deriving (Show)

genBabel :: DataFrame -> Kevin -> Babel
genBabel df kn = Babel
