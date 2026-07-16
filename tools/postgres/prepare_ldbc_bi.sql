SET search_path TO @SCHEMA@;

INSERT INTO Person_knows_Person (creationDate, Person1Id, Person2Id)
SELECT creationDate, Person2Id, Person1Id
FROM Person_knows_Person;

CREATE TABLE City AS
SELECT id, name, url, PartOfPlaceId AS PartOfCountryId
FROM Place
WHERE type = 'City';

CREATE TABLE Country AS
SELECT id, name, url, PartOfPlaceId AS PartOfContinentId
FROM Place
WHERE type = 'Country';

CREATE TABLE Company AS
SELECT id, name, url, LocationPlaceId
FROM Organisation
WHERE type = 'Company';

CREATE TABLE University AS
SELECT id, name, url, LocationPlaceId
FROM Organisation
WHERE type = 'University';

CREATE TABLE Message (
    creationDate TIMESTAMPTZ NOT NULL,
    MessageId BIGINT NOT NULL,
    RootPostId BIGINT NOT NULL,
    RootPostLanguage TEXT,
    content TEXT,
    imageFile TEXT,
    locationIP TEXT NOT NULL,
    browserUsed TEXT NOT NULL,
    length BIGINT NOT NULL,
    CreatorPersonId BIGINT NOT NULL,
    ContainerForumId BIGINT,
    LocationCountryId BIGINT NOT NULL,
    ParentMessageId BIGINT
);

WITH RECURSIVE MessageTree(
    creationDate, MessageId, RootPostId, RootPostLanguage, content, imageFile,
    locationIP, browserUsed, length, CreatorPersonId, ContainerForumId,
    LocationCountryId, ParentMessageId
) AS (
    SELECT creationDate, id, id, language, content, imageFile, locationIP,
           browserUsed, length, CreatorPersonId, ContainerForumId,
           LocationCountryId, NULL::BIGINT
    FROM Post
    UNION ALL
    SELECT c.creationDate, c.id, parent.RootPostId, parent.RootPostLanguage,
           c.content, NULL::TEXT, c.locationIP, c.browserUsed, c.length,
           c.CreatorPersonId, parent.ContainerForumId, c.LocationCountryId,
           coalesce(c.ParentPostId, c.ParentCommentId)
    FROM Comment c
    JOIN MessageTree parent
      ON parent.MessageId = coalesce(c.ParentPostId, c.ParentCommentId)
)
INSERT INTO Message
SELECT * FROM MessageTree;

CREATE TABLE Person_likes_Message AS
SELECT creationDate, PersonId, PostId AS MessageId FROM Person_likes_Post
UNION ALL
SELECT creationDate, PersonId, CommentId AS MessageId FROM Person_likes_Comment;

CREATE TABLE Message_hasTag_Tag AS
SELECT creationDate, PostId AS MessageId, TagId FROM Post_hasTag_Tag
UNION ALL
SELECT creationDate, CommentId AS MessageId, TagId FROM Comment_hasTag_Tag;

CREATE VIEW Comment_View AS
SELECT creationDate, MessageId AS id, locationIP, browserUsed, content, length,
       CreatorPersonId, LocationCountryId, ParentMessageId
FROM Message
WHERE ParentMessageId IS NOT NULL;

CREATE VIEW Post_View AS
SELECT creationDate, MessageId AS id, imageFile, locationIP, browserUsed,
       RootPostLanguage, content, length, CreatorPersonId, ContainerForumId,
       LocationCountryId
FROM Message
WHERE ParentMessageId IS NULL;

CREATE TABLE Top100PopularForumsQ04 AS
SELECT membership.id, Forum.creationDate, membership.maxNumberOfMembers
FROM (
    SELECT ForumId AS id, max(numberOfMembers) AS maxNumberOfMembers
    FROM (
        SELECT fm.ForumId, count(Person.id) AS numberOfMembers,
               City.PartOfCountryId AS CountryId
        FROM Forum_hasMember_Person fm
        JOIN Person ON Person.id = fm.PersonId
        JOIN City ON City.id = Person.LocationCityId
        GROUP BY City.PartOfCountryId, fm.ForumId
    ) ForumMembershipPerCountry
    GROUP BY ForumId
) membership
JOIN Forum ON membership.id = Forum.id;

CREATE TABLE PopularityScoreQ06 AS
SELECT message.CreatorPersonId AS person2id, count(*) AS popularityScore
FROM Message message
JOIN Person_likes_Message likes ON likes.MessageId = message.MessageId
GROUP BY message.CreatorPersonId;

CREATE TABLE PathQ19 AS
WITH weights(src, dst, w) AS (
    SELECT pp.Person1Id, pp.Person2Id,
           greatest(round(40 - sqrt(count(*)))::BIGINT, 1) AS w
    FROM Person_knows_Person pp
    JOIN Message m1 ON true
    JOIN Message m2
      ON pp.Person1Id = least(m1.CreatorPersonId, m2.CreatorPersonId)
     AND pp.Person2Id = greatest(m1.CreatorPersonId, m2.CreatorPersonId)
     AND m1.ParentMessageId = m2.MessageId
     AND m1.CreatorPersonId <> m2.CreatorPersonId
    WHERE pp.Person1Id < pp.Person2Id
    GROUP BY pp.Person1Id, pp.Person2Id
)
SELECT src, dst, w FROM weights
UNION ALL
SELECT dst, src, w FROM weights;

CREATE TABLE PathQ20 AS
SELECT p1.PersonId AS src, p2.PersonId AS dst,
       (min(abs(p1.classYear - p2.classYear)) + 1)::INTEGER AS w
FROM Person_knows_Person pp
JOIN Person_studyAt_University p1 ON pp.Person1Id = p1.PersonId
JOIN Person_studyAt_University p2
  ON pp.Person2Id = p2.PersonId AND p1.UniversityId = p2.UniversityId
GROUP BY p1.PersonId, p2.PersonId;

CREATE INDEX person_id_idx ON Person (id);
CREATE INDEX person_location_idx ON Person (LocationCityId);
CREATE INDEX forum_id_idx ON Forum (id);
CREATE INDEX forum_date_idx ON Forum (creationDate);
CREATE INDEX message_id_idx ON Message (MessageId);
CREATE INDEX message_creator_idx ON Message (CreatorPersonId);
CREATE INDEX message_parent_idx ON Message (ParentMessageId);
CREATE INDEX message_forum_idx ON Message (ContainerForumId);
CREATE INDEX message_date_idx ON Message (creationDate);
CREATE INDEX knows_person1_idx ON Person_knows_Person (Person1Id, Person2Id);
CREATE INDEX knows_person2_idx ON Person_knows_Person (Person2Id, Person1Id);
CREATE INDEX interest_tag_idx ON Person_hasInterest_Tag (TagId, PersonId);
CREATE INDEX forum_member_forum_idx ON Forum_hasMember_Person (ForumId, PersonId);
CREATE INDEX forum_member_person_idx ON Forum_hasMember_Person (PersonId, ForumId);
CREATE INDEX message_tag_message_idx ON Message_hasTag_Tag (MessageId, TagId);
CREATE INDEX message_tag_tag_idx ON Message_hasTag_Tag (TagId, MessageId);
CREATE INDEX likes_message_idx ON Person_likes_Message (MessageId, PersonId);
CREATE INDEX likes_person_idx ON Person_likes_Message (PersonId, MessageId);
CREATE INDEX study_person_idx ON Person_studyAt_University (PersonId, UniversityId);
CREATE INDEX work_company_idx ON Person_workAt_Company (CompanyId, PersonId);
CREATE INDEX pathq19_src_idx ON PathQ19 (src, dst);
CREATE INDEX pathq20_src_idx ON PathQ20 (src, dst);

ANALYZE;
