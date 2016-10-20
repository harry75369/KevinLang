{-# LANGUAGE FlexibleInstances, IncoherentInstances, MultiParamTypeClasses #-}

module Data.DataFrame.Combinator
( SortOrder(..)
, VaridicParam(..)
, VaridicParam2(..)
, PolyParam(..)
, sort
, stack
, unstack
, cast
, height
, width
, size
, take
, drop
, head
, init
, last
, tail
, append
, join
) where

import qualified Prelude as P
import Prelude hiding (take, drop, head, init, last, tail, length, filter)
import qualified Data.HashMap.Strict as M
import Data.DataFrame as DF
import Data.Scientific
import Data.List (sortBy, sortOn, transpose, groupBy, delete)
import qualified Data.List as L
import Data.Maybe (fromJust)

data SortOrder = Ascending | Descending

type FieldsMapping = [(Index, [DataValue])]

class VaridicParam a where
  select :: a -> DataFrame -> DataFrame
  groupby :: a -> DataFrame -> DataFrame

instance VaridicParam FieldName where
  select name df = select [name] df
  groupby name df = groupby [name] df

instance VaridicParam [FieldName] where
  select names (DataFrame indices g _ fields) = DataFrame indices g Nothing fields'
    where
      reorderFields fns fs = fs'
        where
          dict = M.fromList $ zip (map getFieldName fs) fs
          fs' = foldr (\fn acc -> case M.lookup fn dict of
                                    Just field -> field : acc
                                    Nothing -> acc) [] fns
      fields' = reorderFields names fields
  groupby names df@(DataFrame indices _ _ fields) = DataFrame indices g Nothing fields
    where
      DataFrame _ _ _ fs = select names df
      merge :: [FieldMapping] -> FieldsMapping
      merge [] = []
      merge mappings = zip [1..maxIdx] values
        where
          maxIdxs = map (maximum . map fst) mappings
          maxIdx = foldl1 (\m1 m2 -> if m1 /= m2 then error "corrupted data" else m1) maxIdxs
          values = transpose $ map (map snd) $ map (sortOn fst) mappings
      groupOn selector = groupBy (\x y -> selector x == selector y)
      groups :: [FieldsMapping]
      groups = groupOn snd $ sortOn snd $ merge $ map getFieldMapping fs
      groups' = P.filter (not . null) $ map (P.filter inIndices) groups
        where inIndices (i, _) = elem i indices
      reorderByIndices is = P.filter (\i -> elem i is) indices
      g = (names, map (reorderByIndices . map fst) groups')

instance {-# OVERLAPPABLE #-} VaridicParam a where
  select _ _ = error "invalid field name"
  groupby _ _ = error "invalid field name"

class VaridicParam2 a b where
  melt :: a -> b -> DataFrame -> DataFrame

instance VaridicParam2 FieldName FieldName where
  melt idName varName = melt [idName] [varName]

instance VaridicParam2 FieldName [FieldName] where
  melt idName varNames = melt [idName] varNames

instance VaridicParam2 [FieldName] FieldName where
  melt idNames varName = melt idNames [varName]

instance VaridicParam2 [FieldName] [FieldName] where
  melt ids vars df@(DataFrame indices _ _ _) = DataFrame indices' DF.emptyGroups Nothing fs'
    where
      idDf@(DataFrame _ _ _ idFields) = select ids df
      varDf@(DataFrame _ _ _ varFields) = select vars df
      hDf = height df
      nIds = width idDf
      nVars = width varDf
      indices' = [1..hDf*nVars]
      collectValsByIndices dict = foldr (\i acc -> case M.lookup i dict of
                                                     Just val -> val : acc
                                                     Nothing -> acc) [] indices
      idFieldsVals = transpose . concat . replicate nVars . transpose . map transform $ idFields
        where transform = collectValsByIndices . M.fromList . getFieldMapping
      varFieldNames = concat $ map transform varFields
        where transform = replicate hDf . DF.S . getFieldName
      varFieldVals = concat $ map transform varFields
        where transform = collectValsByIndices . M.fromList . getFieldMapping
      fs' = idFields' ++ [varNameField, varValField]
        where
          mkIndices' = zip indices'
          idFieldsMapping = if nVars > 0 && hDf > 0
                               then map mkIndices' idFieldsVals
                               else replicate nIds []
          idFields' = map replaceMapping $ zip idFieldsMapping idFields
            where replaceMapping (mapping, (fn, ft, _)) = (fn, ft, mapping)
          varNameField = ("variable", (Text, Dimension, Discrete), mkIndices' varFieldNames)
          varValField = ("value", (Number, Measure, Continuous), mkIndices' varFieldVals)

instance {-# OVERLAPPABLE #-} VaridicParam2 a b where
  melt _ _ _ = error "invalid field name"

class PolyParam a where
  filter :: FieldName -> (a -> Bool) -> DataFrame -> DataFrame
  aggregate :: ([a] -> a) -> FieldName -> DataFrame -> DataFrame

instance PolyParam String where
  filter fieldName pred = filter' fieldName pred'
    where
      pred' dict i = case M.lookup i dict of
                       Just (DF.S s) -> pred s
                       Just (DF.N n) -> error "inconsistent type"
                       _ -> False
  aggregate op = aggregate' (liftOp op)
    where
      liftOp op = op'
        where
          op' vals = DF.S $ op vals'
            where vals' = foldr collect [] vals
                  collect (DF.S x) l = x : l
                  collect _        _ = error "invalid type"

instance PolyParam Double where
  filter = filterReals
  aggregate = aggregateReals

instance PolyParam Float where
  filter = filterReals
  aggregate = aggregateReals

instance PolyParam Int where
  filter = filterInts
  aggregate = aggregateInts

instance PolyParam Word where
  filter = filterInts
  aggregate = aggregateInts

filterReals :: (RealFloat a) => FieldName -> (a -> Bool) -> DataFrame -> DataFrame
filterReals fieldName pred = filter' fieldName pred'
  where
    pred' dict i = case M.lookup i dict of
                     Just (DF.N n) -> pred (toRealFloat n)
                     Just (DF.S s) -> error "inconsistent type"
                     _ -> False

aggregateReals :: (RealFloat a) => ([a] -> a) -> FieldName -> DataFrame -> DataFrame
aggregateReals op = aggregate' (liftOp op)
  where
    liftOp op = op'
      where
        op' vals = DF.N . fromFloatDigits $ op vals'
          where vals' = foldr collect [] vals
                collect (DF.N x) l = (toRealFloat x) : l
                collect _        _ = error "invalid type"

filterInts :: (Integral a, Bounded a) => FieldName -> (a -> Bool) -> DataFrame -> DataFrame
filterInts fieldName pred = filter' fieldName pred'
  where
    pred' dict i = case M.lookup i dict of
                     Just (DF.N n) -> pred (fromJust . toBoundedInteger $ n)
                     Just (DF.S s) -> error "inconsistent type"
                     _ -> False

aggregateInts :: (Integral a, Bounded a) => ([a] -> a) -> FieldName -> DataFrame -> DataFrame
aggregateInts op = aggregate' (liftOp op)
  where
    liftOp op = op'
      where
        op' vals = DF.N . fromInteger . toInteger $ op vals'
          where vals' = foldr collect [] vals
                collect (DF.N x) l = (fromJust . toBoundedInteger $ x) : l
                collect _        _ = error "invalid type"

filter' :: FieldName -> (M.HashMap Index DataValue -> Index -> Bool) -> DataFrame -> DataFrame
filter' fieldName pred' df@(DataFrame indices _ _ fs) = DataFrame indices' DF.emptyGroups Nothing fs
  where
    dict = case select fieldName df of
      DataFrame _ _ _ [field] -> M.fromList . getFieldMapping $ field
      _ -> error "no such field"
    indices' = P.filter (pred' dict) indices

aggregate' :: ([DataValue] -> DataValue) -> FieldName -> DataFrame -> DataFrame
aggregate' op' fieldName df@(DataFrame indices (ns,gs) _ fs) = DataFrame indices' DF.emptyGroups Nothing fs'
  where
    DataFrame _ _ _ idFields = select ns df
    DataFrame _ _ _ valFields = checkNullField $ select fieldName df
      where checkNullField df
              | width df == 0 = error "no such field"
              | otherwise = df
    indices'
      | null gs = [1]
      | otherwise = [1..P.length gs]
    mergeFieldRow op field@(fieldName, fieldTraits, mapping) = (fieldName, fieldTraits, mapping')
      where
        gs' = if null gs then [indices] else gs
        dict = M.fromList mapping
        valGroups = map getGroupVal gs'
          where
            getGroupVal = flip foldr [] $
              \i acc -> case M.lookup i dict of
                          Just v -> v : acc
                          Nothing -> acc
        mapping' = zip indices' (map op valGroups)
    idFields' = map (mergeFieldRow P.head) idFields
    valFields' = map (mergeFieldRow op') valFields
    fs' = idFields' ++ valFields'

sort :: FieldName -> SortOrder -> DataFrame -> DataFrame
sort fieldName Descending df@(DataFrame indices g t fs) = DataFrame (reverse indices') g Nothing fs
  where DataFrame indices' _ _ _ = sort fieldName Ascending df
sort fieldName Ascending  df@(DataFrame indices g t fs) = DataFrame indices' g Nothing fs
  where
    sorter (_,v0) (_,v1) = compare v0 v1
    inIndices (i, _) = elem i indices
    indices' = case select fieldName df of
      DataFrame _ _ _ [field] -> map fst $ sortBy sorter $ P.filter inIndices $ getFieldMapping field
      _ -> indices

stack :: FieldName -> DataFrame -> DataFrame
stack fieldName (DataFrame indices g Nothing fs) = error "not in pivot form"
stack fieldName (DataFrame indices g (Just (rowTitleTree, colTitleTree)) fs) = result
  where
    fns = map getFieldName fs
    rowFieldNames = getTitleFields rowTitleTree
    colFieldNames = getTitleFields colTitleTree
    result
      | fieldName `elem` colFieldNames, fieldName `elem` fns
      = DataFrame indices g (Just (rowTitleTree', colTitleTree')) fs
      | otherwise = error "unable to stack"
      where
        rowTitleTree' = makeTitleTree $ getFieldsByNames (rowFieldNames ++ [fieldName]) fs
        colTitleTree' = makeTitleTree $ getFieldsByNames (delete fieldName colFieldNames) fs

unstack :: FieldName -> DataFrame -> DataFrame
unstack fieldName (DataFrame indices g Nothing fs) = error "not in pivot form"
unstack fieldName (DataFrame indices g (Just (rowTitleTree, colTitleTree)) fs) = result
  where
    fns = map getFieldName fs
    rowFieldNames = getTitleFields rowTitleTree
    colFieldNames = getTitleFields colTitleTree
    result
      | fieldName `elem` rowFieldNames, fieldName `elem` fns
      = DataFrame indices g (Just (rowTitleTree', colTitleTree')) fs
      | otherwise = error "unable to unstack"
      where
        rowTitleTree' = makeTitleTree $ getFieldsByNames (delete fieldName rowFieldNames) fs
        colTitleTree' = makeTitleTree $ getFieldsByNames (colFieldNames ++ [fieldName]) fs

cast :: (PolyParam a) => [FieldName] -> [FieldName] -> ([a] -> a) -> FieldName -> DataFrame -> DataFrame
cast rowFieldNames colFieldNames agg valFieldName
  | or [i `elem` colFieldNames | i <- rowFieldNames] = error "conflicted field names"
  | valFieldName `elem` allFieldNames = error "conflicted field names"
  | otherwise = unstacks . (toPivot valFieldName) . (aggregate agg valFieldName) . (groupby allFieldNames)
  where
    allFieldNames = rowFieldNames ++ colFieldNames
    unstacks = foldl (.) id $ (map unstack) . reverse $ colFieldNames

height :: DataFrame -> Int
height (DataFrame indices _ Nothing _) = P.length indices
height (DataFrame _ _ (Just (rowTitleTree, _)) _)
  | null (getTitleFields rowTitleTree) = 1
  | otherwise = getChildrenCount rowTitleTree

width :: DataFrame -> Int
width (DataFrame _ _ Nothing fs) = P.length fs
width (DataFrame _ _ (Just (_, colTitleTree)) _)
  | null (getTitleFields colTitleTree) = 1
  | otherwise = getChildrenCount colTitleTree

size :: DataFrame -> (Int, Int)
size = (,) <$> width <*> height

take :: Int -> DataFrame -> DataFrame
take n (DataFrame indices g t fs) = DataFrame indices' g Nothing fs
  where indices' = P.take n indices

drop :: Int -> DataFrame -> DataFrame
drop n (DataFrame indices g t fs) = DataFrame indices' g Nothing fs
  where indices' = P.drop n indices

head :: DataFrame -> DataFrame
head (DataFrame indices g t fs) = DataFrame indices' g Nothing fs
  where indices' = [P.head indices]

init :: DataFrame -> DataFrame
init (DataFrame indices g t fs) = DataFrame indices' g Nothing fs
  where indices' = P.init indices

last :: DataFrame -> DataFrame
last (DataFrame indices g t fs) = DataFrame indices' g Nothing fs
  where indices' = [P.last indices]

tail :: DataFrame -> DataFrame
tail (DataFrame indices g t fs) = DataFrame indices' g Nothing fs
  where indices' = P.tail indices

append :: DataFrame -> DataFrame -> DataFrame
append (DataFrame i0 g0 t0 fs0) (DataFrame i1 g1 t1 fs1)
  | isConsistent (sortOn getFieldName fs0) (sortOn getFieldName fs1)
  = DataFrame indices DF.emptyGroups Nothing (mergeFields fs0 fs1)
  | otherwise = error "Can't append inconsistent data frames."
  where
    isConsistent [] [] = True
    isConsistent ((fn0, ft0, _):fs0) ((fn1, ft1, _):fs1)
      | fn0 == fn1, ft0 == ft1 = isConsistent fs0 fs1
      | otherwise = False
    indices = [1..((P.length i0)+(P.length i1))]
    mergeFields :: [Field] -> [Field] -> [Field]
    mergeFields [] _ = []
    mergeFields ((fn,ft,fm):fs) fs' = f' : mergeFields fs fs'
      where
        fm' = getFieldMapping . P.head $ P.filter (\f -> getFieldName f == fn) fs'
        f' = (fn, ft, mergeFieldMapping fm fm')
    mergeFieldMapping fm0 fm1 = zip indices (v0 ++ v1)
      where
        v0 = map snd $ P.filter (\(i,v) -> i `elem` i0) fm0
        v1 = map snd $ P.filter (\(i,v) -> i `elem` i1) fm1

join :: DataFrame -> DataFrame -> DataFrame
join (DataFrame i0 g0 t0 fs0) (DataFrame i1 g1 t1 fs1)
  | P.length i0 == P.length i1
  = DataFrame indices DF.emptyGroups Nothing (mergeFields fs0 fs1)
  | otherwise = error "Can't join inconsistent data frames."
  where
    indices = [1..(P.length i0)]
    getValsByIndices :: Indices -> FieldMapping -> [DataValue]
    getValsByIndices [] _ = []
    getValsByIndices (i:is) mapping = v : getValsByIndices is mapping
      where v = snd . P.head $ P.filter (\(i',v) -> i == i') mapping
    mergeFields :: [Field] -> [Field] -> [Field]
    mergeFields fs0 fs1 = fs
      where
        vals0 :: [[DataValue]]
        vals0 = map (getValsByIndices i0) $ map getFieldMapping fs0
        vals1 :: [[DataValue]]
        vals1 = map (getValsByIndices i1) $ map getFieldMapping fs1
        mappings :: [FieldMapping]
        mappings = map (zip indices) (vals0 ++ vals1)
        fs = zipWith updateFieldMapping (fs0 ++ fs1) mappings
    updateFieldMapping (fn,ft,fm) fm' = (fn, ft, fm')
