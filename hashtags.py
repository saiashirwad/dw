from config import *


def insert_hashtags(hashtags, t):

	for hashtag in hashtags:

		query = "insert into hashtag_dim (hashtag_text) value ('{}')".format(hashtag)

		try:
			cursor.execute(query)
			db.commit()
			try:
				cursor.execute("select hashtag_dim_id from hashtag_dim where hashtag_text='{}'".format(hashtag))

				try:
					h_d_id = cursor.fetchone()[0]
					print(h_d_id)

					query = "insert into hashtag_tweet_bridge (hashtag_dim_id, tweet_dim_id) values ({}, {})".format(h_d_id, t)

					print("here")
					cursor.execute(query)
					db.commit()

				except Exception as e:
					pass
			except Exception as e:
				pass

		except Exception as e:
			pass





