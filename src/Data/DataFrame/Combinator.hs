{-# LANGUAGE FlexibleInstances, UndecidableInstances #-}

module Data.DataFrame.Combinator where

import Prelude as P
import qualified Data.HashMap.Strict as M
import Data.DataFrame as DF
import Data.Scientific
import Data.List (sortBy)

data SortOrder = Ascending | Descending

class VaridicParam a where
  select :: a -> DataFrame -> DataFrame
  filter :: FieldName -> (a -> Bool) -> DataFrame -> DataFrame
  aggregate :: ([a] -> a) -> FieldName -> DataFrame -> DataFrame

instance VaridicParam [String] where
  select names (DataFrame indices fields) = DataFrame indices fields'
    where fields' = P.filter (\(x,_,_) -> elem x names) fields
  filter _ _ _ = error "invalid type"
  aggregate _ _ _ = error "invalid type"

instance VaridicParam String where
  select name (DataFrame indices fields) = DataFrame indices fields'
    where fields' = P.filter (\(x,_,_) -> x == name) fields
  filter fieldName pred df@(DataFrame indices fs) = (DataFrame indices' fs)
    where
      dict = case select fieldName df of
        DataFrame _ [(_, _, m)] -> M.fromList m
        _ -> M.empty
      pred' i = case M.lookup i dict of
                  Just (DF.S s) -> pred s
                  _ -> False
      indices' = P.filter pred' indices
  aggregate _ _ _ = error "invalid type"

instance {-# OVERLAPPABLE #-} (RealFloat a) => VaridicParam a where
  select _ _ = error "invalid type"
  filter fieldName pred df@(DataFrame indices fs) = (DataFrame indices' fs)
    where
      dict = case select fieldName df of
        DataFrame _ [(_, _, m)] -> M.fromList m
        _ -> M.empty
      pred' i = case M.lookup i dict of
                  Just (DF.N s) -> pred (toRealFloat s)
                  _ -> False
      indices' = P.filter pred' indices
  aggregate op fieldName df = DataFrame [1] [(fieldName, fieldTraits, [(1,val)])]
    where
      DataFrame _ [(_, fieldTraits, mapping)] = select fieldName df
      liftOp op = op'
        where
          op' vals = DF.N . fromFloatDigits $ op vals'
            where vals' = foldl collect [] vals
                  collect l (DF.N x) = (toRealFloat x) : l
                  collect l _        = error "invalid type"
      val = liftOp op $ map snd mapping

sort :: FieldName -> SortOrder -> DataFrame -> DataFrame
sort fieldName Descending df@(DataFrame indices fs) = DataFrame (reverse indices') fs
  where DataFrame indices' _ = sort fieldName Ascending df
sort fieldName Ascending  df@(DataFrame indices fs) = DataFrame indices' fs
  where
    sorter (_,v0) (_,v1) = compare v0 v1
    indices' = case select fieldName df of
      DataFrame _ [(_, _, mapping)] -> map fst $ sortBy sorter mapping
      _ -> indices

mean :: (RealFloat a) => [a] -> a
mean l = sum l / (fromIntegral . length $ l)

count :: (RealFloat b) => [a] -> b
count = fromIntegral . length

sd :: (RealFloat a) => [a] -> a
sd l = mean $ map (sqr . ((-) e)) l
  where e = mean l
        sqr x = x * x
