> import Data.DataFrame.Combinator as C
> import Data.DataFrame.Aggregator as A

> df <- fromCsvFile "simple.csv"
> rowEmptyDf <- C.take 0 df
> colEmptyDf <- C.select "hehehe" df
> allEmptyDf <- C.select "hehehe" $ C.take 0 df

height df -> 13
width df -> 7
size df -> (7, 13)
size rowEmptyDf -> (7, 0)
size colEmptyDf -> (0, 13)
size allEmptyDf -> (0, 0)

C.take (-1) df -> row-empty dataframe
C.take 0 df -> row-empty dataframe
C.take 3 df -> a dataframe
C.take 100 df -> a dataframe

C.head df -> a dataframe
C.head rowEmptyDf -> error "Prelude.head: empty list"
C.head colEmptyDf -> column-empty dataframe

C.tail df -> a dataframe
C.tail rowEmptyDf -> error "Prelude.tail : empty list"
C.tail colEmptyDf -> column-empty dataframe

C.init df -> a dataframe
C.init rowEmptyDf -> error "Prelude.init: empty list"
C.init colEmptyDf -> column-empty dataframe

C.last df -> a dataframe
C.last rowEmptyDf -> error "Prelude.last: empty list"
C.last colEmptyDf -> column-empty dataframe

select "city" df -> a dataframe
select ["city", "cost"] df -> a dataframe
select "hehehe" df -> column-empty dataframe
select ["city", "hehehe"] df -> a dataframe
select 100 df -> error "invalid field name"
select [100, 200] df -> error "invalid field name"
select ["city", 100] df -> GHC error
select [] df -> error "invalid field name"
select ([] :: [FieldName]) df -> column-empty dataframe

groupby "city" df -> a dataframe
groupby ["city", "cost"] df -> a dataframe
groupby "hehehe" df -> a dataframe
groupby ["city", "hehehe"] df -> a dataframe
groupby 100 df -> error "invalid field name"
groupby [100, 200] df -> error "invalid field name"
groupby ["city", 100] df -> GHC error
groupby [] df -> error "invalid field name"
groupby ([] :: [String]) df -> a dataframe

C.filter "city" (== "beijing") df -> a dataframe
C.filter "city" (== 100) df -> error "inconsistent type"
  -- but error "unsupported type"
C.filter "cost" (> 500) df -> a dataframe
  -- but error "unsuported type"
C.filter "cost" (== "hangzhou") -> error "inconsistent type"
C.filter "hehehe" (== "beijing") df -> a dataframe
  -- but row-empty dataframe
C.filter "hehehe" (> 500) df -> a dataframe
  -- but error "unsupported type"
C.filter "hehehe" (> (500 :: Double)) df -> a dataframe
  -- but row-empty dataframe

sort "city" Ascending df -> a dataframe
sort "cost" Descending df -> a dataframe
sort "hehehe" Ascending df -> a dataframe

aggregate Prelude.sum "cost" df -> a dataframe
  -- actually error "unsupported type"
aggregate A.sum "cost" df -> a dataframe
aggregate A.mean "cost" df -> a dataframe
aggregate A.count "cost" df -> a dataframe
aggregate A.variance "cost" df -> a dataframe
aggregate A.sd "cost" df -> a dataframe
aggregate Prelude.sum "city" df -> error "invalid type"
  -- actually error "unsupported type"
aggregate A.sum "city" df -> error "invalid type"
aggregate A.mean "city" df -> error "invalid type"
aggregate A.count "city" df -> a dataframe
  -- acutually error "invalid type"
aggregate A.variance "city" df -> error "invalid type"
aggregate A.sd "city" df -> error "invalid type"
aggregate (concat :: [String] -> String) "city" df -> a dataframe

melt [] [] df -> row-empty dataframe
melt ["city"] [] df -> row-empty dataframe
melt [] ["cost"] df -> a dataframe
melt ["city"] ["cost"] df -> a dataframe
melt [1,2,3] [4,5,6] df -> error "invalid field name"
  -- but GHC error
melt "city" "cost" df -> error "invalid param type"
  -- but GHC error
melt 123 456 df -> error "invalid param type"
  -- but GHC error
melt ["hehehe"] ["hahaha"] df -> row-empty dataframe

-- other TODO
-- 1. make order of fields consistent with combinators' params
-- 2. fix sort comiboator to be consistent with results of row-mutating combinators (take, head, init, tail, last, filter)
-- 3. fix aggregate combinator to be consistent with result of sort combinator

