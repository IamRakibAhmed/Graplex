import sqlite3

conn = sqlite3.connect('spiderWEB.sqlite')
curr = conn.cursor()

curr.execute('''SELECT DISTINCT from_id FROM Links''')
fromIDs = list()
for id in curr:
    fromIDs.append(id[0])

toIDs = list()
link = list()

# -------------Collecting the to_ids and links for page rank---------------------------

curr.execute('''SELECT DISTINCT from_id, to_id FROM Links''')
for row in curr:
    fromID = row[0]
    toID = row[1]

    if fromID == toID:
        continue
    if fromID not in fromIDs:
        continue
    if toID not in fromIDs:
        continue

    link.append(row)
    if toID not in toIDs:
        toIDs.append(toID)

# -------------Getting the latest page rank (depending on the strongly connected pages)-----------------

prevRank = dict()
for Fid in fromIDs:
    curr.execute('''SELECT new_rank FROM Pages WHERE id = ?''', (Fid, ))
    row = curr.fetchone()
    prevRank[Fid] = row[0]

inp = input('Enter the number of attempts: ')
many = 1
if len(inp) > 0:
    many = int(inp)

# ------------Sanity Checking-----------------

if len(prevRank) < 1:
    print('Ranking is not possible at this moment')
    quit()

# -------------Page Rank Algorithm-----------------

for i in range(many):
    newRank = dict()
    total = 0.0
    for (node, oldRank) in list(prevRank.items()):
        total = total + oldRank
        newRank[node] = 0.0

        # Finding outbound links and giving them rank
    for (node, oldRank) in list(prevRank.items()):
        giveIDs = list()
        for (fromID, toID) in link:
            if fromID != node:
                continue
            if toID not in toIDs:
                continue

            giveIDs.append(toID)
        if len(giveIDs) < 1:
            continue
        upRank = oldRank / len(giveIDs)

        for id in giveIDs:
            newRank[id] = newRank[id] + upRank

    newTotal = 0
    for (node, nRank) in list(newRank.items()):
        newTotal = newTotal + nRank

    avg = (total - newTotal) / len(newRank)

    for node in newRank:
        newRank[node] = newRank[node] + avg

    newTotal = 0
    for (node, nRank) in list(newRank.items()):
        newTotal = newTotal + nRank

    # computing the average rank change of each page

    totalDiff = 0
    for (node, oldRank) in list(prevRank.items()):
        nRank = newRank[node]
        diff = abs(oldRank - nRank)
        totalDiff = totalDiff + diff

    avgDiff = totalDiff / len(prevRank)
    print(i+1, avgDiff)
    prevRank = newRank

# --------------set the final rank into the database-----------------

print(list(newRank.items())[:5])
curr.execute('''UPDATE Pages SET old_rank = new_rank''')
for (id, nRank) in list(newRank.items()):
    curr.execute('UPDATE Pages SET new_rank = ? WHERE id = ?', (nRank, id))

conn.commit()
curr.close()
