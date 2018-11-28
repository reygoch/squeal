{-|
Module: Squeal.PostgreSQL.Manipulation
Description: Squeal data manipulation language
Copyright: (c) Eitan Chatav, 2017
Maintainer: eitan@morphism.tech
Stability: experimental

Squeal data manipulation language.
-}

{-# LANGUAGE
    DeriveGeneric
  , FlexibleContexts
  , FlexibleInstances
  , FunctionalDependencies
  , GADTs
  , GeneralizedNewtypeDeriving
  , LambdaCase
  , MultiParamTypeClasses
  , OverloadedStrings
  , PatternSynonyms
  , RankNTypes
  , TypeFamilies
  , TypeInType
  , TypeOperators
  , UndecidableInstances
#-}

module Squeal.PostgreSQL.Manipulation
  ( -- * Manipulation
    Manipulation (UnsafeManipulation, renderManipulation)
  , queryStatement
  , ColumnValue (..)
  , ReturningClause (ReturningStar, Returning)
  , ConflictClause (OnConflictDoRaise, OnConflictDoNothing, OnConflictDoUpdate)
    -- * Insert
  , insertInto
  , insertInto_
  , Insertion (..)
  , renderInsertion
  , pattern Values_
  , Optional (..)
  , DefaultAliasable (..)
  , renderReturningClause
  , renderConflictClause
    -- * Update
  , update
  , update_
    -- * Delete
  , deleteFrom
  , deleteFrom_
  ) where

import Control.DeepSeq
import Data.ByteString hiding (foldr)
import GHC.TypeLits

import qualified Generics.SOP as SOP
import qualified GHC.Generics as GHC

import Squeal.PostgreSQL.Expression
import Squeal.PostgreSQL.Render
import Squeal.PostgreSQL.Query
import Squeal.PostgreSQL.Schema

{- |
A `Manipulation` is a statement which may modify data in the database,
but does not alter the schema. Examples are inserts, updates and deletes.
A `Query` is also considered a `Manipulation` even though it does not modify data.

simple insert:

>>> :{
let
  manipulation :: Manipulation
    '[ "tab" ::: 'Table ('[] :=>
      '[ "col1" ::: 'NoDef :=> 'NotNull 'PGint4
       , "col2" ::: 'Def :=> 'NotNull 'PGint4 ])] '[] '[]
  manipulation = insertInto_ #tab
    (Values_ (2 `as` #col1 :* defaultAs #col2))
in printSQL manipulation
:}
INSERT INTO "tab" ("col1", "col2") VALUES (2, DEFAULT)

parameterized insert:

>>> :{
let
  manipulation :: Manipulation
    '[ "tab" ::: 'Table ('[] :=>
      '[ "col1" ::: 'NoDef :=> 'NotNull 'PGint4
       , "col2" ::: 'NoDef :=> 'NotNull 'PGint4 ])]
    '[ 'NotNull 'PGint4, 'NotNull 'PGint4 ] '[]
  manipulation =
    insertInto_ #tab
      (Values_ ((param @1) `as` #col1 :* (param @2) `as` #col2))
in printSQL manipulation
:}
INSERT INTO "tab" ("col1", "col2") VALUES (($1 :: int4), ($2 :: int4))

returning insert:

>>> :{
let
  manipulation :: Manipulation
    '[ "tab" ::: 'Table ('[] :=>
      '[ "col1" ::: 'NoDef :=> 'NotNull 'PGint4
       , "col2" ::: 'Def :=> 'NotNull 'PGint4 ])] '[]
    '["fromOnly" ::: 'NotNull 'PGint4]
  manipulation =
    insertInto #tab (Values_ (2 `as` #col1 :* defaultAs #col2))
      OnConflictDoRaise (Returning (#col1 `as` #fromOnly))
in printSQL manipulation
:}
INSERT INTO "tab" ("col1", "col2") VALUES (2, DEFAULT) RETURNING "col1" AS "fromOnly"

upsert:

>>> :{
let
  manipulation :: Manipulation
    '[ "tab" ::: 'Table ('[] :=>
      '[ "col1" ::: 'NoDef :=> 'NotNull 'PGint4
       , "col2" ::: 'NoDef :=> 'NotNull 'PGint4 ])]
    '[] '[ "sum" ::: 'NotNull 'PGint4]
  manipulation =
    insertInto #tab (Values
      (2 `as` #col1 :* 4 `as` #col2)
      [6 `as` #col1 :* 8 `as` #col2])
      (OnConflictDoUpdate
        (Set 2 `as` #col1 :* Same `as` #col2)
        [#col1 .== #col2])
      (Returning $ (#col1 + #col2) `as` #sum)
in printSQL manipulation
:}
INSERT INTO "tab" ("col1", "col2") VALUES (2, 4), (6, 8) ON CONFLICT DO UPDATE SET "col1" = 2 WHERE ("col1" = "col2") RETURNING ("col1" + "col2") AS "sum"

query insert:

>>> :{
let
  manipulation :: Manipulation
    '[ "tab" ::: 'Table ('[] :=>
      '[ "col1" ::: 'NoDef :=> 'NotNull 'PGint4
       , "col2" ::: 'NoDef :=> 'NotNull 'PGint4
       ])
     , "other_tab" ::: 'Table ('[] :=>
      '[ "col1" ::: 'NoDef :=> 'NotNull 'PGint4
       , "col2" ::: 'NoDef :=> 'NotNull 'PGint4
       ])
     ] '[] '[]
  manipulation =
    insertInto_ #tab
      (SelectStar (from (table (#other_tab `as` #t))))
in printSQL manipulation
:}
INSERT INTO "tab" SELECT * FROM "other_tab" AS "t"

update:

>>> :{
let
  manipulation :: Manipulation
    '[ "tab" ::: 'Table ('[] :=>
      '[ "col1" ::: 'NoDef :=> 'NotNull 'PGint4
       , "col2" ::: 'NoDef :=> 'NotNull 'PGint4 ])] '[] '[]
  manipulation =
    update_ #tab (Set 2 `as` #col1 :* Same `as` #col2)
      (#col1 ./= #col2)
in printSQL manipulation
:}
UPDATE "tab" SET "col1" = 2 WHERE ("col1" <> "col2")

delete:

>>> :{
let
  manipulation :: Manipulation
    '[ "tab" ::: 'Table ('[] :=>
      '[ "col1" ::: 'NoDef :=> 'NotNull 'PGint4
       , "col2" ::: 'NoDef :=> 'NotNull 'PGint4 ])] '[]
    '[ "col1" ::: 'NotNull 'PGint4
     , "col2" ::: 'NotNull 'PGint4 ]
  manipulation = deleteFrom #tab (#col1 .== #col2) ReturningStar
in printSQL manipulation
:}
DELETE FROM "tab" WHERE ("col1" = "col2") RETURNING *

with manipulation:

>>> type ProductsTable = '[] :=> '["product" ::: 'NoDef :=> 'NotNull 'PGtext, "date" ::: 'Def :=> 'NotNull 'PGdate]

>>> :{
let
  manipulation :: Manipulation
    '[ "products" ::: 'Table ProductsTable
     , "products_deleted" ::: 'Table ProductsTable
     ] '[ 'NotNull 'PGdate] '[]
  manipulation = with
    (deleteFrom #products (#date .< param @1) ReturningStar `as` #deleted_rows)
    (insertInto_ #products_deleted (SelectStar (from (view (#deleted_rows `as` #t)))))
in printSQL manipulation
:}
WITH "deleted_rows" AS (DELETE FROM "products" WHERE ("date" < ($1 :: date)) RETURNING *) INSERT INTO "products_deleted" SELECT * FROM "deleted_rows" AS "t"
-}

newtype Manipulation
  (schema :: SchemaType)
  (params :: [NullityType])
  (columns :: RowType)
    = UnsafeManipulation { renderManipulation :: ByteString }
    deriving (GHC.Generic,Show,Eq,Ord,NFData)
instance RenderSQL (Manipulation schema params columns) where
  renderSQL = renderManipulation
instance With Manipulation where
  with Done manip = manip
  with (cte :>> ctes) manip = UnsafeManipulation $
    "WITH" <+> renderCommonTableExpressions renderManipulation cte ctes
    <+> renderManipulation manip

-- | Convert a `Query` into a `Manipulation`.
queryStatement
  :: Query schema params columns
  -> Manipulation schema params columns
queryStatement q = UnsafeManipulation $ renderQuery q

{-----------------------------------------
INSERT statements
-----------------------------------------}

{- |
When a table is created, it contains no data. The first thing to do
before a database can be of much use is to insert data. Data is
conceptually inserted one row at a time. Of course you can also insert
more than one row, but there is no way to insert less than one row.
Even if you know only some column values, a complete row must be created.
-}
insertInto
  :: ( Has tab schema ('Table table)
     , columns ~ TableToColumns table
     , row ~ TableToRow table
     , SOP.SListI columns
     , SOP.SListI result )
  => Alias tab
  -> Insertion schema params columns
  -> ConflictClause schema table params
  -> ReturningClause schema params row result
  -> Manipulation schema params result
insertInto tab insertion conflict ret = UnsafeManipulation $
  "INSERT" <+> "INTO" <+> renderAlias tab
  <+> renderInsertion insertion
  <> renderConflictClause conflict
  <> renderReturningClause ret

insertInto_
  :: ( Has tab schema ('Table table)
     , columns ~ TableToColumns table
     , row ~ TableToRow table
     , SOP.SListI columns )
  => Alias tab
  -> Insertion schema params columns
  -> Manipulation schema params '[]
insertInto_ tab insertion =
  insertInto tab insertion OnConflictDoRaise (Returning Nil)

data QueryClause schema params columns where
  Values'
    :: NP (ColumnExpression schema '[] 'Ungrouped params) columns
    -> [NP (ColumnExpression schema '[] 'Ungrouped params) columns]
    -> QueryClause schema params columns
  Select'
    :: NP (ColumnExpression schema from grp params) columns
    -> TableExpression schema params from grp
    -> QueryClause schema params columns
  Subquery
    :: ColumnsToRow columns ~ row
    => Query schema params row
    -> QueryClause schema params columns

data ColumnExpression schema from grp params column where
  DefaultAs'
    :: Alias col
    -> ColumnExpression schema from grp params (col ::: 'Def :=> ty)
  Specific'
    :: Aliased (Expression schema from grp params) (col ::: ty)
    -> ColumnExpression schema from grp params (col ::: defness :=> ty)

data ConflictClause' schema params table where
  OnConflictRaise :: ConflictClause' schema params table
  OnConflict
    :: ConflictTarget constraints
    -> ConflictAction schema params columns
    -> ConflictClause' schema params (constraints :=> columns)

data ConflictTarget constraints where
  OnConstraint
    :: Has con constraints constraint
    => Alias con
    -> ConflictTarget constraints

data ConflictAction schema params columns where
  DoNothing :: ConflictAction schema params columns
  DoUpdate
    :: ( row ~ ColumnsToRow columns
       , SOP.SListI columns
       , columns ~ (col0 ': cols)
       , SOP.All (HasIn columns) subcolumns
       , AllUnique subcolumns )
    => NP (ColumnExpression schema '[t ::: row] 'Ungrouped params) subcolumns
    -> [Condition schema '[t ::: row] 'Ungrouped params]
    -> ConflictAction schema params columns

class HasIn fields (x :: (Symbol, a)) where
instance (Has alias fields field) => HasIn fields '(alias, field) where

-- | Utility class for `AllUnique` to provide nicer error messages.
class IsNotElem x isElem where
instance IsNotElem x 'False where
instance (TypeError (      'Text "Cannot assign to "
                      ':<>: 'ShowType alias
                      ':<>: 'Text " more than once"))
   => IsNotElem '(alias, a) 'True where

-- | No elem of @xs@ appears more than once, in the context of assignment.
class AllUnique (xs :: [(Symbol, a)]) where
instance AllUnique '[] where
instance (IsNotElem x (Elem x xs), AllUnique xs) => AllUnique (x ': xs) where

data Insertion schema params columns where
  Values
    :: SOP.SListI columns
    => NP (Optional (Expression schema '[] 'Ungrouped params)) columns
    -> [NP (Optional (Expression schema '[] 'Ungrouped params)) columns]
    -> Insertion schema params columns
  Select
    :: SOP.SListI columns
    => NP (Optional (Expression schema from grp params)) columns
    -> TableExpression schema params from grp
    -> Insertion schema params columns
  SelectStar
    :: (HasUnique table from row, Undefault columns ~ row)
    => TableExpression schema params from 'Ungrouped
    -> Insertion schema params columns

type family Undefault columns where
  Undefault '[] = '[]
  Undefault ((col ::: defness :=> ty) ': columns) = (col ::: ty) ': Undefault columns

renderInsertion
  :: Insertion schema params columns
  -> ByteString
renderInsertion = \case
  Values row0 rows ->
    parenthesized (renderCommaSeparated renderAliasPart row0)
    <+> "VALUES"
    <+> commaSeparated
          ( parenthesized
          . renderCommaSeparated renderValuePart <$> row0 : rows )
  Select row0 tab ->
    parenthesized (renderCommaSeparated renderAliasPart row0)
    <+> "SELECT"
    <+> renderCommaSeparated renderValuePart row0
    <+> renderTableExpression tab
  SelectStar tab -> "SELECT" <+> "*" <+> renderTableExpression tab
  where
    renderAliasPart, renderValuePart
      :: Optional (Expression schema from grp params) column -> ByteString
    renderAliasPart = \case
      DefaultAs name -> renderAlias name
      Specific (_ `As` name) -> renderAlias name
    renderValuePart = \case
      DefaultAs _ -> "DEFAULT"
      Specific (value `As` _) -> renderExpression value

pattern Values_
  :: SOP.SListI columns
  => NP (Optional (Expression schema '[] 'Ungrouped params)) columns
  -> Insertion schema params columns
pattern Values_ vals = Values vals []

data Optional expression column where
  DefaultAs
    :: KnownSymbol col
    => Alias col
    -> Optional expression (col ::: 'Def :=> ty)
  Specific
    :: KnownSymbol col
    => Aliased expression (col ::: ty)
    -> Optional expression (col ::: defness :=> ty)
instance (KnownSymbol alias, column ~ (alias ::: defness :=> ty))
  => Aliasable alias
    (expression ty)
    (Optional expression column)
      where
        expression `as` alias = Specific (expression `As` alias)
instance (KnownSymbol alias, columns ~ '[alias ::: defness :=> ty])
  => Aliasable alias
    (expression ty)
    (NP (Optional expression) columns)
      where
        expression `as` alias = Specific (expression `As` alias) :* Nil
class KnownSymbol alias => DefaultAliasable alias aliased
  | aliased -> alias
  where defaultAs :: Alias alias -> aliased
instance (KnownSymbol col, column ~ (col ::: 'Def :=> ty))
  => DefaultAliasable col (Optional expression column) where
    defaultAs = DefaultAs
instance (KnownSymbol col, columns ~ '[col ::: 'Def :=> ty])
  => DefaultAliasable col (NP (Optional expression) columns) where
    defaultAs col = DefaultAs col :* Nil

-- | `ColumnValue`s are values to insert or update in a row.
-- `Same` updates with the same value.
-- `Default` inserts or updates with the @DEFAULT@ value.
-- `Set` sets a value to be an `Expression`, which can refer to
-- existing value in the row for an update.
data ColumnValue
  (schema :: SchemaType)
  (columns :: RowType)
  (params :: [NullityType])
  (ty :: ColumnType)
  where
    Same :: ColumnValue schema (column ': columns) params ty
    Default :: ColumnValue schema columns params ('Def :=> ty)
    Set
      :: (forall table. Expression schema '[table ::: columns] 'Ungrouped params ty)
      -> ColumnValue schema columns params (constraint :=> ty)

-- | A `ReturningClause` computes and return value(s) based
-- on each row actually inserted, updated or deleted. This is primarily
-- useful for obtaining values that were supplied by defaults, such as a
-- serial sequence number. However, any expression using the table's columns
-- is allowed. Only rows that were successfully inserted or updated or
-- deleted will be returned. For example, if a row was locked
-- but not updated because an `OnConflictDoUpdate` condition was not satisfied,
-- the row will not be returned. `ReturningStar` will return all columns
-- in the row. Use @Returning Nil@ in the common case where no return
-- values are desired.
data ReturningClause
  (schema :: SchemaType)
  (params :: [NullityType])
  (row0 :: RowType)
  (row1 :: RowType)
  where
    ReturningStar
      :: ReturningClause schema params row row
    Returning
      :: NP (Aliased (Expression schema '[table ::: row0] 'Ungrouped params)) row1
      -> ReturningClause schema params row0 row1

-- | Render a `ReturningClause`.
renderReturningClause
  :: SOP.SListI results
  => ReturningClause schema params columns results
  -> ByteString
renderReturningClause = \case
  ReturningStar -> " RETURNING *"
  Returning Nil -> ""
  Returning results -> " RETURNING"
    <+> renderCommaSeparated (renderAliasedAs renderExpression) results

-- | A `ConflictClause` specifies an action to perform upon a constraint
-- violation. `OnConflictDoRaise` will raise an error.
-- `OnConflictDoNothing` simply avoids inserting a row.
-- `OnConflictDoUpdate` updates the existing row that conflicts with the row
-- proposed for insertion.
data ConflictClause
  (schema :: SchemaType)
  (table :: TableType)
  (params :: [NullityType]) where
    OnConflictDoRaise :: ConflictClause schema table params
    OnConflictDoNothing :: ConflictClause schema table params
    OnConflictDoUpdate
      :: (row ~ TableToRow table, columns ~ TableToColumns table)
      => NP (Aliased (ColumnValue schema row params)) columns
      -> [Condition schema '[t ::: row] 'Ungrouped params]
      -> ConflictClause schema table params

-- | Render a `ConflictClause`.
renderConflictClause
  :: SOP.SListI (TableToColumns table)
  => ConflictClause schema table params
  -> ByteString
renderConflictClause = \case
  OnConflictDoRaise -> ""
  OnConflictDoNothing -> " ON CONFLICT DO NOTHING"
  OnConflictDoUpdate updates whs'
    -> " ON CONFLICT DO UPDATE SET"
      <+> renderCommaSeparatedMaybe renderUpdate updates
      <> case whs' of
        [] -> ""
        wh:whs -> " WHERE" <+> renderExpression (foldr (.&&) wh whs)
      where
        renderUpdate
          :: Aliased (ColumnValue schema columns params) column
          -> Maybe ByteString
        renderUpdate = \case
          Same `As` _ -> Nothing
          Default `As` column -> Just $
            renderAlias column <+> "=" <+> "DEFAULT"
          Set expression `As` column -> Just $
            renderAlias column <+> "=" <+> renderExpression expression

{-----------------------------------------
UPDATE statements
-----------------------------------------}

-- | An `update` command changes the values of the specified columns
-- in all rows that satisfy the condition.
update
  :: ( SOP.SListI columns
     , SOP.SListI results
     , Has tab schema ('Table table)
     , row ~ TableToRow table
     , columns ~ TableToColumns table )
  => Alias tab -- ^ table to update
  -> NP (Aliased (ColumnValue schema row params)) columns
  -- ^ modified values to replace old values
  -> (forall t. Condition schema '[t ::: row] 'Ungrouped params)
  -- ^ condition under which to perform update on a row
  -> ReturningClause schema params row results -- ^ results to return
  -> Manipulation schema params results
update tab columns wh returning = UnsafeManipulation $
  "UPDATE"
  <+> renderAlias tab
  <+> "SET"
  <+> renderCommaSeparatedMaybe renderUpdate columns
  <+> "WHERE" <+> renderExpression wh
  <> renderReturningClause returning
  where
    renderUpdate
      :: Aliased (ColumnValue schema columns params) column
      -> Maybe ByteString
    renderUpdate = \case
      Same `As` _ -> Nothing
      Default `As` column -> Just $
        renderAlias column <+> "=" <+> "DEFAULT"
      Set expression `As` column -> Just $
        renderAlias column <+> "=" <+> renderExpression expression

-- | Update a row returning `Nil`.
update_
  :: ( SOP.SListI columns
     , Has tab schema ('Table table)
     , row ~ TableToRow table
     , columns ~ TableToColumns table )
  => Alias tab -- ^ table to update
  -> NP (Aliased (ColumnValue schema row params)) columns
  -- ^ modified values to replace old values
  -> (forall t. Condition schema '[t ::: row] 'Ungrouped params)
  -- ^ condition under which to perform update on a row
  -> Manipulation schema params '[]
update_ tab columns wh = update tab columns wh (Returning Nil)

{-----------------------------------------
DELETE statements
-----------------------------------------}

-- | Delete rows of a table.
deleteFrom
  :: ( SOP.SListI results
     , Has tab schema ('Table table)
     , row ~ TableToRow table
     , columns ~ TableToColumns table )
  => Alias tab -- ^ table to delete from
  -> Condition schema '[tab ::: row] 'Ungrouped params
  -- ^ condition under which to delete a row
  -> ReturningClause schema params row results -- ^ results to return
  -> Manipulation schema params results
deleteFrom tab wh returning = UnsafeManipulation $
  "DELETE FROM" <+> renderAlias tab
  <+> "WHERE" <+> renderExpression wh
  <> renderReturningClause returning

-- | Delete rows returning `Nil`.
deleteFrom_
  :: ( Has tab schema ('Table table)
     , row ~ TableToRow table
     , columns ~ TableToColumns table )
  => Alias tab -- ^ table to delete from
  -> (forall t. Condition schema '[t ::: row] 'Ungrouped params)
  -- ^ condition under which to delete a row
  -> Manipulation schema params '[]
deleteFrom_ tab wh = deleteFrom tab wh (Returning Nil)
