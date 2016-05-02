# Sample Spark Program

A co-clicked pair of items refers to two items that have been clicked by the same user. The idea is that if lots of people are co-clicking items then a recommender system can suggest to a new user that they buy item 2 if they click on item 1. This program determines the number of co-clicked pairs of items by users.

The input is thus a tuple of (user_id, item_id of a item that the user clicked).
The output is a tuple of ((item1, item2), count of users that co-clicked item1 and item2). Note that the output is filtered so that it only includes co-clicked items that have at least 3 users.


The steps the program takes to do so are:

1. Read data in as pairs of (user_id, item_id clicked on by the user)
2. Group data into (user_id, list of item ids they clicked on)
3. Transform into (user_id, (item1, item2) where item1 and item2 are pairs of items the user clicked on
4. Transform into ((item1, item2), list of user1, user2 etc) where users are all the ones who co-clicked (item1, item2)
5. Transform into ((item1, item2), count of distinct users who co-clicked (item1, item2)
6. Filter out any results where less than 3 users co-clicked the same pair of items

To run the program open two terminals:
$ sudo docker-compose up
$ make
