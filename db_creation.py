import json
import sqlite3
import string
from langdetect import detect
from urlextract import URLExtract

database_path = ''
combined_file_path = ''

print('\n\n     CREATING SQLITE DATABASE      \n\n')

sql_t = []
number_of_transactions = 0
conn = sqlite3.connect(database_path)
cursor = conn.cursor()

cursor.execute("""CREATE TABLE IF NOT EXISTS comment_pairs(id INT PRIMARY KEY,
                                                                parent_body TEXT,
                                                                reply_body TEXT,
                                                                subreddit TEXT,
                                                                parent_length INT,
                                                                reply_length INT)""")

cursor.execute("""CREATE TABLE IF NOT EXISTS parents(id TEXT PRIMARY KEY,
                                                          parent_body TEXT,
                                                          subreddit TEXT,
                                                          parent_length INT)""")

cursor.execute("""CREATE TABLE IF NOT EXISTS replies(id TEXT PRIMARY KEY,
                                                          reply_body TEXT,
                                                          reply_length INT)""")

# go through the data

row_counter = 0  # counter for the rows
post_pairs = {}

a = string.printable + '’‘“”'
eng_char = set(a)

extractor = URLExtract()

def acceptable_comment(body):
    """basic test to see if comment is not too short or too long"""
#     if (len(body.split(' ')) > 100) or (len(body) < 4):
    if (len(body.split(' ')) > 15) or (len(body) < 4):
        return False
    elif len(body) > 3000:
        return False
    elif (body == '[deleted]' or body == '[removed]' or body.isspace()==True):
        return False
    elif len(extractor.find_urls(body))>0:
        return False
    elif not(set(body).issubset(eng_char)):
        return False
    else:
        try:
            language = detect(body)
        except:
            return False
        if language != 'en':
            return False
        else:
            return True

def clear_comment(body):
    """basic test if a comment is insulting"""
    if (('fuck' or 'shit' or 'ass' or 'bitch' or 'nigga' or 'hell' or 'whore' or 'dick' or 'piss' or 'pussy') in body.lower()):
        return False
    else:
        return True
        
def sanitize_body(body):
    """sanitize body of reddit posts"""
    body = body.replace('\n', ' newlinechar ')
    body = body.replace('\r', ' newlinechar ')
    body = body.replace('"', "'")
    while '....' in body:
        body=body.replace('....','...') 
    while '  ' in body:
        body = body.replace('  ',' ')
    while 'newlinechar newlinechar' in body:
        body=body.replace('newlinechar newlinechar', 'newlinechar ') 
    return body


def transaction_bldr(sql):
    global sql_t
    global number_of_transactions
    sql_t.append(sql)
    if number_of_transactions > 10000:
        if len(sql_t) == 10000:
            cursor.execute('BEGIN TRANSACTION')
            for s in sql_t:
                cursor.execute(s)
            conn.commit()
            sql_t = []
            number_of_transactions-=10000
    else:
        cursor.execute('BEGIN TRANSACTION')
        for s in sql_t:
            cursor.execute(s)
        conn.commit()
        sql_t = []

        
def sql_insert_comment_pair(id, parent_body, reply_body, subreddit, parent_length, reply_length):
    sql = """INSERT INTO comment_pairs (id, parent_body, reply_body, subreddit, parent_length, reply_length) VALUES ({},"{}","{}","{}",{}, {});""".format(
        id, parent_body, reply_body, subreddit, parent_length, reply_length)
    transaction_bldr(sql)

def sql_insert_parent(id, parent_body, subreddit, parent_length):
    sql = """INSERT INTO parents (id, parent_body, subreddit, parent_length) VALUES ({},"{}","{}",{});""".format(
        id, parent_body, subreddit, parent_length)
    transaction_bldr(sql)
    
def sql_insert_reply(id, reply_body, reply_length):
    sql = """INSERT INTO replies (id, reply_body, reply_length) VALUES ({},"{}",{});""".format(
        id, reply_body, reply_length)
    transaction_bldr(sql)
    
print('\n CREATE COMMENT PAIRS DICTIONAIRIES \n')    
    
with open(combined_file_path, buffering=1000000) as f:
    for row in f:
        row_counter += 1
        if row_counter % 1000000 == 0:
            print('Working on row {}'.format(row_counter))
        row = json.loads(row)
        score = row['score']
        
        if score>2:
            comment_body = sanitize_body(row['body'])
            if acceptable_comment(comment_body): 
                if clear_comment(comment_body):
                    id = row['id']
                    parent_id = row['parent_id'].split('_')[1]
                    score = row['score']
                    subreddit = row['subreddit']
                    length = len(comment_body.split(' '))

                    if parent_id not in post_pairs:
                        post_pairs[parent_id] = (id, score)
                    else:
                        other_id, other_score = post_pairs[parent_id]
                        if score > other_score:
                            post_pairs[parent_id] = (id, score)


ids = set(x[0] for x in post_pairs.values())
print('\n * \n Comments in comment table with or without a reply:' + str(len(ids)) + '\n * \n')     


clean_parents_ids = [i for i in ids if i in post_pairs.keys()] #dhl apo ta apodekta, auta pou einai kai goneis.
                                                               #ara 8a dextoume osa exoun gia id ta clean_parent_ids -->parents                                                               
print(len(clean_parents_ids))

clean_post_pairs = {}
inverse_clean_post_pairs = {}

for i, cl_pid in enumerate(clean_parents_ids):
    clean_post_pairs[cl_pid] = (i, post_pairs[cl_pid][0])
    inverse_clean_post_pairs[post_pairs[cl_pid][0]] = (i, cl_pid)

# ###
# cursor.execute('''SELECT subreddit FROM comment_pairs''')
# ps = cursor.fetchall()
# ###

# # #
# for i, cl_pid in enumerate(clean_parents_ids):
#     clean_post_pairs[cl_pid] = (len(ps) + i, post_pairs[cl_pid][0])
#     inverse_clean_post_pairs[post_pairs[cl_pid][0]] = (len(ps) + i, cl_pid)
# # #

clean_ids = [x for x in inverse_clean_post_pairs.keys()]

print('Number of comment pairs: '+ str(len(clean_ids)))
number_of_transactions = 2*len(clean_ids)


print('\n INSERT COMMENT IN SEPARATE TABLES \n') 
row_counter = 0
# row_counter_2 = 0
insert_counter = 0
clean_set = set(clean_ids)
clean_parent_set = set(clean_parents_ids)

with open(combined_file_path, buffering=1000000) as f:
    for row in f:
        row_counter += 1
        
        if row_counter % 1000000 == 0:
            print('Working on row {}'.format(row_counter))
#             print('Insert counter {}'.format(insert_counter))
        
        row = json.loads(row)
        id = row['id']
        
        if id in clean_set:
            insert_id = inverse_clean_post_pairs[id][0]
            reply_body = sanitize_body(row['body'])
            reply_length = len(reply_body.split(' '))
            
            sql_insert_reply(insert_id, reply_body, reply_length)
            insert_counter += 1
            
        if id in clean_parent_set:
            insert_id = clean_post_pairs[id][0]
            parent_body = sanitize_body(row['body'])
            parent_length = len(parent_body.split(' '))
            subreddit = row['subreddit']
            
            sql_insert_parent(insert_id, parent_body, subreddit, parent_length)
            insert_counter += 1
            
print('Insert counter {}'.format(insert_counter))

print('\n INSERT COMMENT PAIRS IN COMMENT_PAIRS TABLE \n') 

cursor.execute("""INSERT INTO comment_pairs
SELECT parents.id, parents.parent_body, replies.reply_body, parents.subreddit, parents.parent_length, replies.reply_length 
FROM parents
INNER JOIN replies ON
parents.id = replies.id""")

cursor.execute("""DROP TABLE parents""")
cursor.execute("""DROP TABLE replies""")