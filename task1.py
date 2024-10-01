from mrjob.job import MRJob
from mrjob.step import MRStep
#the MRStep in MRJob allows to create a sequence of mapper and reducer steps to have the output of one step be used as the input for the next step


class MostProfitableProducts(MRJob):
    
    def steps(self):
        #Step 1 is the mapper class and the reducer class to get the total revenue
        #Step 2 is to yield the top 5 highest total revenues
        return [MRStep(mapper=self.mapperclass, reducer=self.reducer_sum_revenue), 
                MRStep(reducer=self.reducer_top_5)]
    
    def mapperclass(self,_,line):
		#split the line into fields
        fields = line.split(',')
        if fields[0] != 'TransactionID': #skip the header row, row with the column names
            product_id = fields[2] #get product id
            revenue = float(fields[4]) #get the revenue
            yield product_id, revenue #set the id as key and revenue as value

    def reducer_sum_revenue(self, product_id, revenues): #receives all the revenues associated with each productID to sum them up
        #aggregate the total revenue for each product id
        #None is to prepare for global sorting and have all the data sent to a single reducer
        #yield None, (product_id, sum(revenues))
        #place the total revenue before the id to faciliate sorting the products by revenue
        yield None, (sum(revenues), product_id)

    def reducer_top_5(self,_,product_revenues):
        #get all the revenue and product id tuples
        #python will sort the first element of each tuple, so sort by revenue
        top5 = sorted(product_revenues, reverse=True)[:5] #sort all the total revenues in descending order and get the first 5
        for revenue, product_id in top5: #iterate through to yield the top 5
            yield 'Product ID', product_id
            yield 'Total Revenue', revenue


if __name__ == '__main__':
    MostProfitableProducts.run()
