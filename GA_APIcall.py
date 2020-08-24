#import dependencies
import pandas as pd
#Load Libraries
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
import httplib2


#create api_connect
def connect_to_api(clientKey='client_secrets.json'):
    #Rename your JSON key to client_secrets.json and save it to your working folder
    credentials = ServiceAccountCredentials.from_json_keyfile_name(clientKey, ['https://www.googleapis.com/auth/analytics.readonly'])
    #Create a service object
    http = credentials.authorize(httplib2.Http())
    service = build('analytics', 'v4', http=http, discoveryServiceUrl=('https://analyticsreporting.googleapis.com/$discovery/rest'))
    return service




##Set function for API request

def GA_api_call(viewId, 
				service,
                dimensions = ['ga:medium'], 
                metrics = ['ga:sessions'],
                startDate = '30daysAgo', 
                endDate = 'today', 
                orderBy = None, #if not filled, orders by first metric
                sortOrder = 'DESCENDING',               
                filterExpression = None,
                segment = None,
                includeEmptyRows = True,
                samplingLevel = 'LARGE',
                pageSize = 10000,
                pageToken = '0',
                nextPage = True,
               ): 

    #If number of rows exceeds pageSize, loop to get full result
    responseList = []
    while nextPage:

        
        ##Format call details:

        #Make dictionary list out of given dimensions and metrics
        #dimensions":
        if segment == None:
            dimensionDict = [{'name':dim} for dim in dimensions] 
        else:
            dimensionDict = [{'name':dim} for dim in dimensions] + [{'name':'ga:segment'}]
        #metrics:
        metricsDict = [{'expression':met} for met in metrics]

        #Set row order details
        if orderBy == None:
            orderBy = metricsDict[0]['expression']

        #Set segments
        if segment == None:
            segmentId = None
        else:
            segmentId = [{'segmentId':segment}]




        ##Make call to API
        response = service.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId':viewId, #Add View ID from GA
                        'dateRanges':[{'startDate':startDate, 'endDate':endDate}],
                        'dimensions':dimensionDict, 
                        'metrics':metricsDict, 
                        'filtersExpression':filterExpression,
                        'orderBys':[{'fieldName':orderBy, 'sortOrder':sortOrder}], 
                        'segments':segmentId,
                        'includeEmptyRows':includeEmptyRows, 
                        'samplingLevel':samplingLevel,
                        'pageSize':pageSize, #max number of rows per call is 100000
                        'pageToken':pageToken
                    }]
            }
        ).execute()
        
####        next_page = response.reports[0]    
        
        if len(response['reports'][0]) == 2:
            nextPage = False
            response = responseList + [response]
        else:
            pageToken = response['reports'][0]['nextPageToken']
            responseList = responseList + [response]
        
    return response
	

	
	
##Extract data from API response

def parse_api_response(callResponse):
    #create two empty lists that will hold metricsHeaders and list of all datarows
    dataList = []

    dfList = []

    for i in range(len(callResponse)):
    
        #Extract Data
        for report in callResponse[i].get('reports', []):
            #Get headers
            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricDetails = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
            metricHeaders = []
            for i in range(len(metricDetails)):
                metricHeaders.append(metricDetails[i]['name'])
            headers = dimensionHeaders + metricHeaders
            #Get column dTypes
            dimensionTypes = ['category' for dim in dimensionHeaders]
            metricTypes = []
            for i in range(len(metricDetails)):
                mtype = metricDetails[i]['type']
                if mtype == 'METRIC_TYPE_UNSPECIFIED':
                    metricType = 'object'
                elif mtype == 'INTEGER':
                    metricType = 'int64'
                elif mtype == FLOAT:
                    metricType = 'float64'
                elif mtype == 'CURRENCY':
                    metricType = 'float64'
                elif mtype == 'PERCENT':
                    metricType = 'float64'
                elif mtype == 'TIME':
                    metricType = 'datetime64'                    
                metricTypes.append(metricType)
            columnTypes = dimensionTypes + metricTypes
            typeDict = dict(zip(headers, columnTypes))

            rows = report.get('data', {}).get('rows', [])

            for row in rows:

                dimensions = row.get('dimensions', [])
                metrics = row.get('metrics', [])[0].get('values')
                rowlist = dimensions + metrics
                dataList.append(rowlist)

        #Change the dataList object into a dataframe.      
        df = pd.DataFrame(dataList)
        df.columns = headers
        df = df.astype(typeDict) 
        df.columns = df.columns.str.lstrip('ga:')
        
        #Append dataframes to a list
        dfList.append(df)
    
    df_compleet = pd.concat(dfList)
                
    
    return df