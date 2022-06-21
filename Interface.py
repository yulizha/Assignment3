import psycopg2

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    num = []
    cursor = openconnection.cursor()
    sql = '''
        SELECT PartitionNum 
        FROM RangeRatingsMetadata
        WHERE MaxRating>={0} AND MinRating<={1}
        '''.format(ratingMinValue, ratingMaxValue)
    cursor.execute(sql)
    result = cursor.fetchall()
    result = [data[0] for data in result]

    for data in result:
        sql1 = '''
            SELECT *
            FROM RangeRatingsPart{0}
            WHERE Rating >={1} AND Rating <={2}
        '''.format(data, ratingMinValue, ratingMaxValue)
        cursor.execute(sql1)
        table = cursor.fetchall()

        for inputdata in table:
            inputdata = list(inputdata)
            inputdata.insert(0,'RangeRatingsPart{}'.format(data))
            num.append(inputdata)

    rounddata = '''
        SELECT PartitionNum 
        FROM RoundRobinRatingsMetadata
    '''
    cursor.execute(rounddata)
    fromrounddata = cursor.fetchall()[0][0]

    for i in xrange(0,fromrounddata):
        sql2 = '''
            SELECT *
            FROM RoundRobinRatingsPart{0}
            WHERE Rating >={1} AND Rating <={2}
        '''.format(i,ratingMinValue, ratingMaxValue)
        cursor.execute(sql2)
        table = cursor.fetchall()

        for inputdata in table:
            inputdata = list(inputdata)
            inputdata.insert(0,'RangeRatingsPart{}'.format(i))
            num.append(inputdata)

    writeToFile('RangeQueryOut.txt', num)

def PointQuery(ratingsTableName, ratingValue, openconnection):
    num = []
    cursor = openconnection.cursor()
    sql = '''
        SELECT PartitionNum 
        FROM RangeRatingsMetadata
        WHERE MinRating <= {0} AND MaxRating >= {0}
    '''.format(ratingValue)
    cursor.execute(sql)
    result = cursor.fetchall()
    result = [data[0] for data in result]

    for data in result:
        sql1 = '''
            SELECT *
            FROM RangeRatingsPart{0}
            WHERE Rating = {1} 
        '''.format(data, ratingValue)
        cursor.execute(sql1)
        table = cursor.fetchall()

        for inputdata in table:
            inputdata = list(inputdata)
            inputdata.insert(0,'RangeRatingsPart{}'.format(data))
            num.append(inputdata)

    rounddata = '''
        SELECT PartitionNum 
        FROM RoundRobinRatingsMetadata
    '''
    cursor.execute(rounddata)
    fromrounddata = cursor.fetchall()[0][0]

    for i in xrange(0,fromrounddata):
        sql2 = '''
            SELECT *
            FROM RoundRobinRatingsPart{0}
            WHERE Rating = {1}
        '''.format(i,ratingValue)
        cursor.execute(sql2)
        table = cursor.fetchall()

        for inputdata in table:
            inputdata = list(inputdata)
            inputdata.insert(0,'RangeRatingsPart{}'.format(i))
            num.append(inputdata)

    writeToFile('PointQueryOut.txt', num)

def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
