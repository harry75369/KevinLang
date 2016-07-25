module CsvParser where

import Text.Megaparsec
import Text.Megaparsec.String
import qualified Text.Megaparsec.Lexer as L
import Data.Scientific

type Header = [String]
type Record = [Val]
data Val = S String | N Scientific | Empty deriving (Show)

csvParser :: Parser (Header, [Record])
csvParser = do
  header <- csvHeader <* eol
  records <- sepBy csvRecord eol
  eof >> return (header, records)

csvHeader :: Parser Header
csvHeader = sepBy stringVal csvDelimiter

csvRecord :: Parser Record
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
