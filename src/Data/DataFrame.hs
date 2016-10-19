module Data.DataFrame
( TitleTree(..)
, TitleChild(..)
, TitleTrees
, getTitleFields
, getTitleValue
, getTitleIndices
, HasChildren(..)
, DataFrame(..)
, Indices
, Index
, Groups
, Field
, FieldName
, FieldTraits
, FieldMapping
, DataType(..)
, DataRole(..)
, DataInterpretation(..)
, DataValue(..)
, fromCsvFile
, emptyGroups
, getFieldName
, getFieldTraits
, getFieldMapping
, getFieldsByNames
, makeTitleTree
, fromPivot
, toPivot
) where

import CsvParser
import Data.Char (isSpace, isLatin1)
import Data.List (transpose, nub, sort, delete)
import Text.Megaparsec
import Data.Scientific
import qualified Data.HashMap.Strict as M

data TitleTree = TitleTree [FieldName] [TitleChild] deriving Show
data TitleChild = TitleChild DataValue Indices [TitleChild] deriving Show
type TitleTrees = Maybe (TitleTree, TitleTree)

getTitleFields   (TitleTree fs _) = fs
getTitleValue    (TitleChild v _ _) = v
getTitleIndices  (TitleChild _ i _) = i

class HasChildren a where
  getTitleChildren :: a -> [TitleChild]
  getChildrenCount :: a -> Int
  getTreeLeaves :: a -> [TitleChild]

instance HasChildren TitleTree where
  getTitleChildren (TitleTree _ children) = children
  getChildrenCount (TitleTree _ children) = sum $ map getChildrenCount children
  getTreeLeaves    (TitleTree _ children) = concat $ map getTreeLeaves children

instance HasChildren TitleChild where
  getTitleChildren (TitleChild _ _ children) = children
  getChildrenCount (TitleChild _ _ []) = 1
  getChildrenCount (TitleChild _ _ children) = sum $ map getChildrenCount children
  getTreeLeaves    leaf@(TitleChild _ _ []) = [leaf]
  getTreeLeaves    (TitleChild _ _ children) = concat $ map getTreeLeaves children

data DataFrame = DataFrame Indices Groups TitleTrees [Field]
type Indices = [Index]
type Index   = Int
type Groups = ([FieldName], [Indices])
type Field  = (FieldName, FieldTraits, FieldMapping)
type FieldName = String
type FieldTraits = (DataType, DataRole, DataInterpretation)
type FieldMapping = [(Index, DataValue)]
data DataType = Text | Number | Date | Time | DateTime | Geography deriving (Show, Eq)
data DataRole = Dimension | Measure deriving (Show, Eq)
data DataInterpretation = Discrete | Continuous deriving (Show, Eq)
data DataValue = S String | N Scientific | Empty

instance Eq DataValue where
  (Data.DataFrame.S s0) == (Data.DataFrame.S s1) = s0 == s1
  (Data.DataFrame.N n0) == (Data.DataFrame.N n1) = n0 == n1
  Data.DataFrame.Empty == Data.DataFrame.Empty = True
  _ == _ = False

instance Ord DataValue where
  compare (Data.DataFrame.S s0) (Data.DataFrame.S s1) = compare s0 s1
  compare (Data.DataFrame.N n0) (Data.DataFrame.N n1) = compare n0 n1
  compare _                     _                     = error "invalid data values for sorting"

instance Show DataValue where
  show (Data.DataFrame.S s) = s
  show (Data.DataFrame.N n) = show n
  show (Data.DataFrame.Empty) = "empty"

showLines ls = concat $ map (showLine widths) ls
  where
    columns = transpose ls
    widths = map maximum $ map (map strLength) columns
    showLine (w:_)  (x:[]) = (space $ w - strLength x + 2) ++ x ++ "\n"
    showLine (w:ws) (x:xs) = (space $ w - strLength x + 2) ++ x ++ showLine ws xs
    space = flip replicate ' '
    strLength [] = 0
    strLength (c:cs)
      | isLatin1 c = 1 + strLength cs
      | otherwise  = 2 + strLength cs

instance Show DataFrame where
  show (DataFrame _       _ _       []    ) = "(EMPTY DATAFRAME)\n"
  show (DataFrame indices g Nothing fields) = (showLines $ header : records) ++ (showGroups g)
    where
      traits = map getFieldTraits fields
      dicts = map (M.fromList . getFieldMapping) fields
      showVal i (trait, dict) =
        let (_,_,dataInterpreation) = trait in
        case M.lookup i dict of
          Just (Data.DataFrame.N n) ->
            if dataInterpreation == Discrete
               then formatScientific Fixed (Just 0) n
               else show n
          Just dataValue -> show dataValue
          Nothing -> ""
      showRecord i = (show i) : map (showVal i) (zip traits dicts)
      showGroups (ns, gs)
        | null ns || null gs = ""
        | otherwise = unlines $ show ns : map show gs

      header = "" : map (\(x,_,_) -> x) fields
      records = [showRecord i | i <- indices]
  show (DataFrame indices g (Just (rowTitleTree, colTitleTree)) fields) = showLines combined
    where
      rowFieldNames = getTitleFields rowTitleTree
      colFieldNames = getTitleFields colTitleTree
      fieldNames = map getFieldName fields
      [valFieldName] = filter (\n -> not (n `elem` rowFieldNames || n `elem` colFieldNames)) fieldNames
      [valField] = filter (\f -> getFieldName f == valFieldName) fields
      valFieldMapping = getFieldMapping valField

      getVal :: Indices -> Indices -> DataValue
      getVal rowIndices colIndices
        | null colFieldNames = getVal' rowIndices
        | null rowFieldNames = getVal' colIndices
        | otherwise = getVal' [i | i <- rowIndices, i `elem` colIndices]
        where
          getVal' [] = Data.DataFrame.Empty
          getVal' [idx] = snd $ head $ filter (\(i,v) -> i == idx) valFieldMapping
          getVal' _ = error "invalid data (many values for the same index)"

      addTitlePadding :: Int -> [String] -> [String]
      addTitlePadding n (x:xs) = x : (replicate n "") ++ xs

      showTitleTree :: TitleTree -> [[String]]
      showTitleTree (TitleTree fieldNames children) = iter fieldNames children
        where
          iter [] _ = []
          iter (n:ns) children = thisLine : iter ns children'
            where
              showValue (Data.DataFrame.N n) = formatScientific Fixed (Just 0) n
              showValue dv                   = show dv
              makeValue c@(TitleChild dataValue indices children) = showValue dataValue : replicate (getChildrenCount c - 1) ""
              thisLine = n : (concat $ map makeValue children)
              children' = concat $ map getTitleChildren children

      rowTitleStrs = transpose $ showTitleTree rowTitleTree
      colTitleStrs = map (addTitlePadding ((length $ transpose rowTitleStrs) - 1)) $ showTitleTree colTitleTree
      rowLeaves = getTreeLeaves rowTitleTree
      colLeaves = getTreeLeaves colTitleTree
      contentStrs
        | null rowLeaves = [[show $ getVal [] colIndices | TitleChild _ colIndices _ <- colLeaves]]
        | null colLeaves = [[show $ getVal rowIndices []] | TitleChild _ rowIndices _ <- rowLeaves]
        | otherwise = [[show $ getVal rowIndices colIndices | TitleChild _ colIndices _ <- colLeaves] | TitleChild _ rowIndices _ <- rowLeaves]
      combined
        | null rowTitleStrs = colTitleStrs ++ zipWith (++) [[""]] contentStrs
        | otherwise = colTitleStrs ++ zipWith (++) rowTitleStrs ((replicate (length colLeaves) "") : contentStrs)

    --in unlines (map unwords colTitleStrs)
    --   ++ unlines (map unwords rowTitleStrs)
    --   ++ unlines (map unwords contentStrs)
    --   ++ show colTitleTree
    --   ++ ('\n' : show rowTitleTree)
    --   ++ ('\n' : show colTitleStrs)
    --   ++ ('\n' : show rowTitleStrs)

fromCsvFile :: FilePath -> IO DataFrame
fromCsvFile filePath = do
  let trim = f . f where f = reverse . dropWhile isSpace
  content <- trim <$> readFile filePath

  fromCsv <$> case parse csvParser filePath content of
    Left err -> error (parseErrorPretty err)
    -- Right r@(h, rs) -> print h >> mapM_ print rs >> return r
    Right r -> return r

emptyGroups = ([],[])

fromCsv :: (CsvHeader, [CsvRecord]) -> DataFrame
fromCsv (header, records) = DataFrame indices emptyGroups Nothing fields
  where
    indices = [1..length records]
    fields = map (makeField indices) $ zip header $ transpose records

makeField :: Indices -> (FieldName, [CsvVal]) -> Field
makeField indices (fieldName, vals) = (fieldName, traits, mappings)
  where
    isCsvString (CsvParser.S _) = True
    isCsvString _               = False
    isCsvNumber (CsvParser.N _) = True
    isCsvNumber _               = False
    isDateTime  _               = False
    isDate      _               = False
    isTime      _               = False
    isGeography _               = False
    traits
      | all isCsvString vals
      , all isDateTime vals
      = (DateTime, Dimension, Continuous)
      | all isCsvString vals
      , all isDate vals
      = (Date, Dimension, Discrete)
      | all isCsvString vals
      , all isTime vals
      = (Time, Dimension, Continuous)
      | all isCsvString vals
      , all isGeography vals
      = (Geography, Dimension, Discrete)
      | all isCsvString vals
      = (Text, Dimension, Discrete)
      | all isCsvNumber vals
      , fieldName == "year"
      = (Number, Dimension, Discrete)
      | all isCsvNumber vals
      = (Number, Measure, Continuous)
      | otherwise
      = error "\nError: Invalid data (possibly missing values)"
    mappings = zip indices $ map convert vals
      where convert (CsvParser.S s) = Data.DataFrame.S s
            convert (CsvParser.N n) = Data.DataFrame.N n
            convert CsvParser.Empty = Data.DataFrame.Empty

getFieldName :: Field -> FieldName
getFieldName (fieldName,_,_) = fieldName

getFieldTraits :: Field -> FieldTraits
getFieldTraits (_,fieldTraits,_) = fieldTraits

getFieldMapping :: Field -> FieldMapping
getFieldMapping (_,_,fieldMapping) = fieldMapping

getFieldsByNames :: [FieldName] -> [Field] -> [Field]
getFieldsByNames fns fs = foldl f [] fns
  where f acc fieldName = acc ++ filter (\field -> getFieldName field == fieldName) fs

makeTitleTree :: [Field] -> TitleTree
makeTitleTree fs = TitleTree fns (makeTitleChildren fms)
  where
    fns = map getFieldName fs
    fms = map getFieldMapping fs
    makeTitleChildren :: [FieldMapping] -> [TitleChild]
    makeTitleChildren [] = []
    makeTitleChildren (m:ms) = map (makeTitleChild m ms) vals
      where
        vals = sort . nub $ map snd m
    makeTitleChild :: FieldMapping -> [FieldMapping] -> DataValue -> TitleChild
    makeTitleChild m ms val = TitleChild val indices (makeTitleChildren ms')
      where
        indices = map fst $ filter (\(i,v) -> v == val) m
        ms' = map (filter (\(i,v) -> i `elem` indices)) ms

fromPivot :: DataFrame -> DataFrame
fromPivot (DataFrame indices g _ fs) = DataFrame indices g Nothing fs

toPivot :: FieldName -> DataFrame -> DataFrame
toPivot fieldName (DataFrame indices g _ fs)
  | fieldName `elem` fns = DataFrame indices g (Just (rowTitleTree, colTitleTree)) fs
  | otherwise = error "please provide correct field to pivot"
  where
    fns = map getFieldName fs
    rowTitleTree = makeTitleTree $ getFieldsByNames (delete fieldName fns) fs
    colTitleTree = makeTitleTree []

