/* Q18. Friend recommendation
\set tag '\'Frank_Sinatra\''
 */
WITH
PersonWithInterest AS (
SELECT pt.PersonId as PersonId
FROM Person_hasInterest_Tag pt, Tag t
WHERE t.name = :tag and pt.TagId = t.id
),
FriendsOfInterested AS (
SELECT k.Person1Id AS InterestedId, k.Person2Id AS FriendId
FROM PersonWithInterest p, Person_knows_Person k
WHERE p.PersonId = k.Person1Id
)
SELECT k1.InterestedId AS "person1.id", k2.InterestedId AS "person2.id", count(k1.FriendId) AS mutualFriendCount
FROM FriendsOfInterested k1
JOIN FriendsOfInterested k2
  ON k1.FriendId = k2.FriendId -- pattern: mutualFriend
WHERE k1.InterestedId != k2.InterestedId
  -- The reference query uses a correlated NOT EXISTS here. Expressing the
  -- same negative edge as a row-value NOT IN avoids a DuckDB optimizer error.
  AND (k2.InterestedId, k1.InterestedId) NOT IN (
        SELECT Person1Id, Person2Id FROM Person_knows_Person
      )
GROUP BY k1.InterestedId, k2.InterestedId
-- LIMIT on this anti-filter plan triggers a DuckDB TopN optimizer failure.
-- QUALIFY preserves the reference query's deterministic top-20 semantics.
QUALIFY row_number() OVER (
  ORDER BY count(k1.FriendId) DESC, k1.InterestedId ASC, k2.InterestedId ASC
) <= 20
ORDER BY mutualFriendCount DESC, k1.InterestedId ASC, k2.InterestedId ASC
;
