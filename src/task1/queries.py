query1 = """
CREATE TABLE Players(Player_ID INTEGER PRIMARY KEY, Player_Name VARCHAR, \
Rank VARCHAR, level_ID INTEGER);
CREATE TABLE Levels(Level_ID INTEGER PRIMARY KEY, Level_Name VARCHAR);
"""

query2 = """
INSERT INTO Players VALUES (13,'Raven','Cadet',48), (25,'CryHavoc','Lieutenant',51 ), \
(37, 'oolala', 'Lieutenant', 17), (443, 'TheSquid', 'Colonel', 89), (509, 'meh', 'Cadet', 48);
INSERT INTO Levels VALUES (48, 'Caverns of Doom'), (51,'Lake of the Undead'), \
(17,'Forest of Evil Things'), (89,'Island of Darkness');
"""


query3 = """
SELECT count(*) as num_players, Rank \
FROM Players \
JOIN Levels ON (Players.Level_ID = Levels.Level_ID) \
GROUPBY LevelName;
"""


query4 = """
SELECT * FROM Players
"""
