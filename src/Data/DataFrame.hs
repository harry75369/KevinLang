module Data.DataFrame where

import CsvParser
import Data.Char (isSpace)
import Text.Megaparsec
import Data.Scientific

data DataFrame = DataFrame Indices [Column]
type Index   = Int
type Indices = [Index]
type Column  = (ColumnTraits, Index -> DataValue)
type ColumnTraits = (DataType, DataRole, DataInterpretation)
data DataType = Text | Number | Date | Time | DateTime | Geography
data DataRole = Dimension | Measure
data DataInterpretation = Discrete | Continuous
data DataValue = S String | N Scientific | Empty

fromCsvFile :: FilePath -> IO ()
fromCsvFile filePath = do
  let trim = f . f where f = reverse . dropWhile isSpace
  content <- trim <$> readFile filePath
  case parse csvParser filePath content of
    Left err -> putStr (parseErrorPretty err)
    Right (header, records) -> print header >> mapM_ print records
