{-# LANGUAGE FlexibleInstances #-}

module Data.DataFrame.Combinator
( SortOrder(..)
, VaridicParam(..)
, PolyParam(..)
, sort
) where

import Prelude as P
import qualified Data.HashMap.Strict as M
import Data.DataFrame as DF
import Data.Scientific
import Data.List (sortBy, sortOn, transpose, groupBy)
import Data.Maybe (fromJust)

data SortOrder = Ascending | Descending

class VaridicParam a where
  select :: a -> DataFrame -> DataFrame
  groupby :: a -> DataFrame -> DataFrame

instance VaridicParam String where
  select name df = select [name] df
  groupby name df = groupby [name] df

instance VaridicParam [String] where
  select names (DataFrame indices g fields) = DataFrame indices g fields'
    where fields' = P.filter (\(x,_,_) -> elem x names) fields
  groupby names df@(DataFrame indices _ fields) = DataFrame indices g fields
    where
      DataFrame _ _ fs = select names df
      merge :: [[(Index, DataValue)]] -> [(Index, [DataValue])]
      merge mappings = zip [1..maxIdx] values
        where
          maxIdxs = map (maximum . map fst) mappings
          maxIdx = foldl1 (\m1 m2 -> if m1 /= m2 then error "corrupted data" else m1) maxIdxs
          values = transpose $ map (map snd) $ map (sortOn fst) mappings
      groupOn selector = groupBy (\x y -> selector x == selector y)
      groups :: [[(Index, [DataValue])]]
      groups = groupOn snd $ sortOn snd $ merge $ map (\(_,_,m) -> m) fs
      g = (names, map (map fst) groups)

instance {-# OVERLAPPABLE #-} VaridicParam a where
  select _ _ = error "invalid field name"
  groupby _ _ = error "invalid field name"

class PolyParam a where
  filter :: FieldName -> (a -> Bool) -> DataFrame -> DataFrame
  aggregate :: ([a] -> a) -> FieldName -> DataFrame -> DataFrame

instance PolyParam String where
  filter fieldName pred = filter' fieldName pred'
    where
      pred' dict i = case M.lookup i dict of
                  Just (DF.S s) -> pred s
                  _ -> False
  aggregate op = aggregate' (liftOp op)
    where
      liftOp op = op'
        where
          op' vals = DF.S $ op vals'
            where vals' = foldl collect [] vals
                  collect l (DF.S x) = x : l
                  collect l _        = error "invalid type"

instance PolyParam Double where
  filter fieldName pred = filter' fieldName pred'
    where
      pred' dict i = case M.lookup i dict of
                  Just (DF.N s) -> pred (toRealFloat s)
                  _ -> False
  aggregate op = aggregate' (liftOp op)
    where
      liftOp op = op'
        where
          op' vals = DF.N . fromFloatDigits $ op vals'
            where vals' = foldl collect [] vals
                  collect l (DF.N x) = (toRealFloat x) : l
                  collect l _        = error "invalid type"

instance PolyParam Float where
  filter fieldName pred = filter' fieldName pred'
    where
      pred' dict i = case M.lookup i dict of
                  Just (DF.N s) -> pred (toRealFloat s)
                  _ -> False
  aggregate op = aggregate' (liftOp op)
    where
      liftOp op = op'
        where
          op' vals = DF.N . fromFloatDigits $ op vals'
            where vals' = foldl collect [] vals
                  collect l (DF.N x) = (toRealFloat x) : l
                  collect l _        = error "invalid type"

instance PolyParam Int where
  filter fieldName pred = filter' fieldName pred'
    where
      pred' dict i = case M.lookup i dict of
                  Just (DF.N s) -> pred (fromJust . toBoundedInteger $ s)
                  _ -> False
  aggregate op = aggregate' (liftOp op)
    where
      liftOp op = op'
        where
          op' vals = DF.N . fromInteger . toInteger $ op vals'
            where vals' = foldl collect [] vals
                  collect l (DF.N x) = (fromJust . toBoundedInteger $ x) : l
                  collect l _        = error "invalid type"

instance PolyParam Word where
  filter fieldName pred = filter' fieldName pred'
    where
      pred' dict i = case M.lookup i dict of
                  Just (DF.N s) -> pred (fromJust . toBoundedInteger $ s)
                  _ -> False
  aggregate op = aggregate' (liftOp op)
    where
      liftOp op = op'
        where
          op' vals = DF.N . fromInteger . toInteger $ op vals'
            where vals' = foldl collect [] vals
                  collect l (DF.N x) = (fromJust . toBoundedInteger $ x) : l
                  collect l _        = error "invalid type"

--instance {-# OVERLAPPABLE #-} PolyParam a where
--  filter _ _ _ = error "unsupported type"
--  aggregate _ _ _ = error "unsupported type"

filter' fieldName pred' df@(DataFrame indices _ fs) = DataFrame indices' DF.emptyGroups fs
  where
    dict = case select fieldName df of
      DataFrame _ _ [(_, _, m)] -> M.fromList m
      _ -> M.empty
    indices' = P.filter (pred' dict) indices

aggregate' op' fieldName df@(DataFrame indices (ns,gs) fs) = DataFrame indices' DF.emptyGroups fs'
  where
    DataFrame _ _ idFields = select ns df
    DataFrame _ _ [valField] = select fieldName df
    indices'
      | null gs = [1]
      | otherwise = [1..length gs]
    mergeFieldRow op field@(fieldName, fieldTraits, mapping) = (fieldName, fieldTraits, mapping')
      where
        gs' = if null gs then [indices] else gs
        valGroups = map getGroupVal gs'
          where getGroupVal g = map snd $ P.filter (\(i,v) -> elem i g) mapping
        mapping' = zip indices' (map op valGroups)
    idFields' = map (mergeFieldRow head) idFields
    valField' = mergeFieldRow op' valField
    fs' = idFields' ++ [valField']

sort :: FieldName -> SortOrder -> DataFrame -> DataFrame
sort fieldName Descending df@(DataFrame indices g fs) = DataFrame (reverse indices') g fs
  where DataFrame indices' _ _ = sort fieldName Ascending df
sort fieldName Ascending  df@(DataFrame indices g fs) = DataFrame indices' g fs
  where
    sorter (_,v0) (_,v1) = compare v0 v1
    indices' = case select fieldName df of
      DataFrame _ _ [(_, _, mapping)] -> map fst $ sortBy sorter mapping
      _ -> indices

