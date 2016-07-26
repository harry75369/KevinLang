module CsvParser where

import Text.Megaparsec
import Text.Megaparsec.String
import qualified Text.Megaparsec.Lexer as L
import Data.Scientific

type CsvHeader = [String]
type CsvRecord = [CsvVal]
data CsvVal = S String | N Scientific | Empty deriving (Show)

csvParser :: Parser (CsvHeader, [CsvRecord])
csvParser = do
  header <- csvHeader <* eol
  records <- sepBy csvRecord eol
  eof >> return (header, records)

csvHeader :: Parser CsvHeader
csvHeader = sepBy stringVal csvDelimiter

csvRecord :: Parser CsvRecord
csvRecord = flip sepBy csvDelimiter $
      try (N <$> numberVal)
  <|> try (S <$> stringVal)
  <|> return Empty

numberVal :: Parser Scientific
numberVal = L.number

stringVal :: Parser String
stringVal = escapedStringVal <|> unescapedStringVal

escapedStringVal :: Parser String
escapedStringVal = between csvQuote csvQuote $ many (normal <|> escaped)
  where normal  = noneOf "\""
        escaped = '"' <$ string "\"\""

unescapedStringVal :: Parser String
unescapedStringVal = some $ noneOf ",\"\n\r"

csvDelimiter :: Parser Char
csvDelimiter = char ','

csvQuote :: Parser Char
csvQuote = char '"'
