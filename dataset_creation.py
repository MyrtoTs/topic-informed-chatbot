import sqlite3

database_path = ''

conn = sqlite3.connect(database_path)
cursor = conn.cursor()

cursor.execute("""SELECT p.parent_body, p.reply_body, p.subreddit FROM comment_pairs p JOIN
                ( SELECT subreddit FROM comment_pairs GROUP BY subreddit HAVING COUNT(subreddit) > 1000 ) b 
                ON p.subreddit = b.subreddit""")

ps = cursor.fetchall()

counter = 0
parent1 = open('train.from', 'w', buffering=10000)
reply1 = open('train.to', 'w', buffering=10000)
parent2 = open('tst2012.from','w')
reply2 = open('tst2012.to','w')
parent3 = open('tst2013.from','w')
reply3 = open('tst2013.to','w')

labels1 = open('labels.train', 'w', buffering=10000)
labels2 = open('labels.dev','w')
labels3 = open('labels.tst','w')

for parent_body, reply_body, subreddit in ps:
    print(counter)
    if counter<10000:
        parent2.write(parent_body + '\n')
        reply2.write(reply_body +'\n')
        labels2.write(subreddit + '\n')  
    if 10000<=counter<20000:
        parent3.write(parent_body + '\n')
        reply3.write(reply_body +'\n')
        labels3.write(subreddit + '\n')  
    if 20000<=counter<len(ps):
        parent1.write(parent_body + '\n')
        reply1.write(reply_body +'\n')
        labels1.write(subreddit + '\n')   
    counter +=1

parent1.close()
parent2.close()
parent3.close()
reply1.close()
reply2.close()
reply3.close()
labels1.close()
labels2.close()
labels3.close()
