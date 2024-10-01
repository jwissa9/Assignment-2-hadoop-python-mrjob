from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class MostActiveHour(MRJob):
    
    def steps(self):
        #Step 1: Get the hour from the mapper and count the transactions within that hour with reducer
        #Step 2: Get the max count from an hour
        return [MRStep(mapper=self.mapper_hour, reducer=self.reducer_count), #step 1
                MRStep(reducer=self.reducer_max_hour)] #step 2
        
    def mapper_hour(self,_,line): #get each line as input
        fields = line.split(',') #Split off the lines by comma
        if fields[0] != 'TransactionID': #skip the first row
            timestamp = fields[5] #get the timestamp
            hour = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').hour #extract the hour from the timestamp with parsing
            yield hour, 1 #emit the hour and the count of 1 for couting the transactions within the hour
        
    def reducer_count(self,hour,count): #receives all counts for each hour
        #sums up all the counts to get the total amount of transactions for each hour
        #emits a tuple that places the transaction count before the hour so that the next reducer function can look for and select the highest count
        #None helps with global comparisions
        yield None, (sum(count), hour)
        
    def reducer_max_hour(self,_,hour_count): #gets all the tuples with the transaction count and hour
        max_hour = max(hour_count) #selects the tuple that has the highest transaction count
        yield 'Most Active Hour', max_hour[1] 
        yield 'Number of Transactions', max_hour[0] #shows the hour and the transaction count
        

if __name__ == '__main__':
    MostActiveHour.run()
