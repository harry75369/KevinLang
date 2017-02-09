module Language.Kevin
( Kevin(..)
, module Language.Kevin.Marks
, module Language.Kevin.Visuals
, module Language.Kevin.Scales
, module Language.Kevin.Coords
) where

import Language.Kevin.Marks
import Language.Kevin.Visuals
import Language.Kevin.Scales
import Language.Kevin.Coords

data Kevin = Kevin {
  knCoord  :: Maybe Coord
, knMark   :: Maybe Mark
}

instance Monoid Kevin where
  mempty = Kevin Nothing Nothing
  mappend (Kevin a b) (Kevin Nothing Nothing) = Kevin a b
  mappend (Kevin Nothing Nothing) (Kevin c d) = Kevin c d
  mappend (Kevin a Nothing) (Kevin c Nothing) = error "coordinate already declared"
  mappend (Kevin Nothing b) (Kevin Nothing d) = error "mark already declared"
  mappend (Kevin a Nothing) (Kevin Nothing d) = Kevin a d
  mappend (Kevin Nothing b) (Kevin c Nothing) = Kevin c b
  mappend (Kevin a b) (Kevin c d) = error "conflicted declaration"

