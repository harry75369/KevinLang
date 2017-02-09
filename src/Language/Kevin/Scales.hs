{-# LANGUAGE TypeSynonymInstances, FlexibleInstances #-}
module Language.Kevin.Scales
( Scale(..)
) where

import Data.DataFrame

data Scale = CScale | DScale deriving (Show)

class ScaleVardicParam a where
  linear :: a -> Scale
  category :: a -> Scale

instance ScaleVardicParam FieldName where
  linear fn = undefined
  category fn = undefined

instance ScaleVardicParam (FieldName -> [String]) where
  linear f = error "orders are not for linear scales"
  category f = undefined

