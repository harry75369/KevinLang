module Data.DataFrame where

import CsvParser
import Data.Char (isSpace)
import Data.List (transpose)
import Text.Megaparsec
import Data.Scientific
import qualified Data.HashMap.Strict as M

data DataFrame = DataFrame Indices [Field]
type Index   = Int
type Indices = [Index]
type Field  = (String, FieldTraits, [(Index, DataValue)])
type FieldTraits = (DataType, DataRole, DataInterpretation)
data DataType = Text | Number | Date | Time | DateTime | Geography deriving (Show, Eq)
data DataRole = Dimension | Measure deriving (Show, Eq)
data DataInterpretation = Discrete | Continuous deriving (Show, Eq)
data DataValue = S String | N Scientific | Empty

instance Show DataFrame where
  show (DataFrame indices fields) = showLines $ header : records
    where
      traits = map (\(_,x,_) -> x) fields
      dicts = map (\(_,_,x) -> M.fromList x) fields
      showVal i (trait, dict) =
        let (_,_,dataInterpreation) = trait in
        case M.lookup i dict of
          Just (Data.DataFrame.S s) -> s
          Just (Data.DataFrame.N n) ->
            if dataInterpreation == Discrete
               then formatScientific Fixed (Just 0) n
               else show n
          Just Data.DataFrame.Empty -> "empty"
          Nothing -> "empty"
      showRecord i = (show i) : map (showVal i) (zip traits dicts)
      showLines ls = concat $ map (showLine widths) ls
        where
          columns = transpose ls
          widths = map maximum $ map (map length) columns
          showLine (w:_)  (x:[]) = (space $ w - length x + 2) ++ x ++ "\n"
          showLine (w:ws) (x:xs) = (space $ w - length x + 2) ++ x ++ showLine ws xs
          space n = take n $ iterate id ' '

      header = "" : map (\(x,_,_) -> x) fields
      records = [showRecord i | i <- indices]

fromCsvFile :: FilePath -> IO DataFrame
fromCsvFile filePath = do
  let trim = f . f where f = reverse . dropWhile isSpace
  content <- trim <$> readFile filePath

  fromCsv <$> case parse csvParser filePath content of
    Left err -> error (parseErrorPretty err)
    -- Right r@(h, rs) -> print h >> mapM_ print rs >> return r
    Right r -> return r

fromCsv :: (CsvHeader, [CsvRecord]) -> DataFrame
fromCsv (header, records) = DataFrame indices fields
  where
    indices = [1..length records]
    fields = map (makeField indices) $ zip header $ transpose records

makeField :: Indices -> (String, [CsvVal]) -> Field
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

