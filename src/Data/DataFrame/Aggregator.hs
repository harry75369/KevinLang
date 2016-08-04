module Data.DataFrame.Aggregator where

import Prelude hiding (sum)
import qualified Prelude as P

sum :: (RealFloat a) => [a] -> a
sum = P.sum

mean :: (RealFloat a) => [a] -> a
mean l = sum l / (fromIntegral . length $ l)

count :: (RealFloat b) => [a] -> b
count = fromIntegral . length

variance :: (RealFloat a) => [a] -> a
variance l = mean $ map (sqr . ((-) e)) l
  where e = mean l
        sqr x = x * x

sd :: (RealFloat a) => [a] -> a
sd = sqrt . variance

