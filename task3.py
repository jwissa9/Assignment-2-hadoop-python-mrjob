from mrjob.job import MRJob
from mrjob.step import MRStep

class TotalSalesAndAvgRevenue(MRJob):
    
    def steps(self):
        #Step 1: Use the mapper and reducer to sum up the quantity and revenue
        #Step 2: Use the revenue from the previous step to calculate the average
        return [MRStep(mapper=self.mappers_qr, reducer=self.reducer_sum_q_and_r), #step 1
                MRStep(reducer=self.reducer_average_revenue)] #step 2
        
    def mappers_qr(self,_,line): #Mapper that gets the input lines
        fields = line.split(',') #Splits off each line by the comma
        if fields[0] != 'TransactionID': #skip the first row
            category = fields[1] #get the product category
            quantity = int(fields[3]) #get the quantity sold
            revenue = float(fields[4]) #get the revenue generated
            yield category, (quantity, revenue) #emit product category with its quantity and revenue as values within a tuple
        
    def reducer_sum_q_and_r(self, category, values): #get all the tuples from the mapper function with the category and the values
        total_q = 0
        total_r = 0.0
        count = 0
        #gets the quantity and revenue as values to sum up their total for each product category as well as count the product categories found
        for q, r in values: #iterate through the tuples to acculmulate the quantity and revenue and count the transactions within the category
            total_q += q
            total_r += r
            count += 1 #the count will be used to calculate the average revenue
        #emit the category with its tuple consisting the total quanity and revenue as well as the count of transactions
        yield category, (total_q, total_r, count)
                
    def reducer_average_revenue(self, category, values): #gets all the aggregated values for each category
        for total_q, total_r, count in values: #iterates through the values, though there is only one per category
            average_r = total_r / count #calculate the average revenue with the summed up revenue and count
            yield category, {'Total Quantity Sold': total_q, 'Average Revenue': average_r} #emit the results


if __name__ == '__main__':
    TotalSalesAndAvgRevenue.run()
