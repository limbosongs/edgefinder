import pymongo

if __name__ == '__main__':
    print 'Clearing mongo...'
    connection = pymongo.MongoClient('localhost', 27017)
    db = connection['horse-racing']
    collection = db['odds-scraper']
    collection.drop()
    print 'Successfully cleared mongo.'
