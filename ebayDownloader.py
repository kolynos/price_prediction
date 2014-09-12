'''
Created on 09.09.2014

@author: colinos
'''
import datetime

from ebaysdk.finding import Connection as finding
from ebaysdk.trading import Connection as trading
from ebaysdk.shopping import Connection as shopping
from requests.exceptions import ConnectionError
from copy import copy,deepcopy
#import os
import os.path
import pickle
#from ebaysdk.soa.finditem import Connection as FindItem




def connect_api():
    global trading_api,finding_api,shopping_api
    trading_api=trading(config_file='ebay.yaml',siteid=77)
    finding_api=finding(config_file='ebay.yaml',siteid='EBAY-DE')
    shopping_api=shopping(config_file='ebay.yaml',siteid=77)
    




def get_categories():
    global trading_api
    try:
        response = trading_api.execute('GetCategories',{'CategorySiteID':77,'DetailLevel':'ReturnAll','CategoryParent':58058})
        print(response.dict())
    except ConnectionError as e:
        print(e)
        print(e.response.dict())

def get_item_complete_description(item):
    global trading_api
    trading_api.execute('GetItem', {'ItemID':item})
    print trading_api.response_dict()
    
def get_item_descriptions():
    global shopping_api
    a=2

def get_category_aspects(category):
    global finding_api
    
    category_aspects=None
    filename='category_aspects_'+str(category)+'.pkl'
    if(os.path.isfile(filename)):
        pkl_file=open(filename, 'r')
        category_aspects=pickle.load(pkl_file)
        pkl_file.close()
    else:
        try:
            category_aspects=dict()
            response=finding_api.execute('findItemsByCategory',{'categoryId':category,'outputSelector':'AspectHistogram'})
            aspects=response.dict()['aspectHistogramContainer']['aspect']
            print(aspects)
            for i in range(len(aspects)):
                print(aspects[i]['_name'])
                category_aspects[aspects[i]['_name']]=list()
                values=aspects[i]['valueHistogram']
                for j in range(len(values)):
                    category_aspects[aspects[i]['_name']].append(values[j]['_valueName'])
                    print(values[j]['_valueName'])
            
            print(category_aspects)
            output = open('category_aspects_'+str(category)+'.pkl', 'w')
            pickle.dump(category_aspects,output,-1)
            output.close()
        except ConnectionError as e:
            print(e)
            print(e.response.dict())
    keys=category_aspects.keys()
    for i in range(len(keys)):
        #print str(i)+' '+keys[i]
        values=category_aspects[keys[i]]
        #for j in range(len(values)):
        #    print(values[j])
        
    #print(category_aspects)
    return category_aspects


def get_aspect_filter(aspect_list):
    aspect_list=[{'aspectName':aspect.keys()[0],'aspectValueName':aspect[aspect.keys()[0]]} for aspect in aspect_list]
    #aspect_dict['aspectFilter']=aspect_list
    return aspect_list
    #for aspect in keys:
        

def recursive_find(category,aspect_history,category_counter,query_type='findItemsByCategory'):
    global category_aspects, category_order
    
    print aspect_history
    if(category_counter==3):
        aspect_filter=get_aspect_filter(aspect_history)
        print(aspect_filter)
        get_valid_aspects(category,aspect_filter)
        return
    current_category=category_order[category_counter]
    current_name=category_aspects.keys()[current_category]
    aspect_filter=get_aspect_filter(aspect_history)
    valid_aspects=get_valid_aspects(category,aspect_filter)
    #if(current_name in valid_aspects.keys()):
    #print valid_aspects
    if(current_name in valid_aspects.keys()):
        for aspect in category_aspects[current_name]:
            print aspect
            new_history=copy(aspect_history)        
            if(aspect in valid_aspects[current_name]):
                aspect_dict={current_name:aspect}
                new_history.append(aspect_dict)
                recursive_find(category,new_history,category_counter+1)
    else:
        new_history=copy(aspect_history)
        recursive_find(category,new_history,category_counter+1)
    
def get_valid_aspects(category,aspect_filter,query_type='findItemsByCategory'):
    global valid_aspect_categories
    
    query=dict()
    query['outputSelector']='AspectHistogram'
    query['categoryId']=category
    query['aspectFilter']=aspect_filter
    response=finding_api.execute(query_type,query)
    result=response.dict()
    valid_aspects=dict()
    if(result.has_key('aspectHistogramContainer')):
        aspects=result['aspectHistogramContainer']['aspect']
        for aspect in aspects:
            #print aspect['_name']
            #print valid_aspect_categories
            #print aspect['_name'] in valid_aspect_categories
            
            #if(aspect['_name'] in valid_aspect_categories):
            
                #print(aspect['_name'])
            valid_aspects[aspect['_name']]=list()
            values=aspect['valueHistogram']
            for value in values:
                valid_aspects[aspect['_name']].append(value['_valueName'])
                #print(values[j]['_valueName'])
            
            #if(valid_aspect_categories.containaspect_category['_name'])
    return valid_aspects
    
    #result=response.dict()
    

def get_items_by_category(category,category_aspects,category_order,query_type='findItemsByCategory'):
    global finding_api
    try:
        
        #response = finding_api.execute('findItemsByCategory',{'categoryId':111422,'aspectFilter':{'Erscheinungsjahr',2013}})
        #response=finding_api.execute('findItemsByCategory',{'categoryId':111422,'aspectFilter':{'aspectName':'Erscheinungsjahr','aspectValueName':['2011','2012']}})
        
        
        keys=category_aspects.keys()
        for c in category_order:
            aspects=category_aspects[keys[c]]
            print keys[c]
            for a in range(len(aspects)):
                print aspects[a]
                response=finding_api.execute(query_type,{'outputSelector':'AspectHistogram','sortOrder':'StartTimeNewest','categoryId':category,'aspectFilter':{'aspectName':keys[c],'aspectValueName':aspects[a]}})
                aspects=response.dict()['aspectHistogramContainer']['aspect']
                print(aspects)
                result=response.dict()
                print(result['paginationOutput'])
                print(result['itemSearchURL']) 
    
        
        #=======================================================================
        # print(category_aspects.keys()[1])
        # print(category_aspects[category_aspects.keys()[1]][2])
        # 
        # response=finding_api.execute(query_type,{'sortOrder':'StartTimeNewest','categoryId':category,'aspectFilter':{'aspectName':category_aspects.keys()[1],'aspectValueName':category_aspects[category_aspects.keys()[1]][2]}})
        # 
        # result=response.dict()
        # print(result['paginationOutput'])
        # print(result['itemSearchURL'])
        # #sprint(result['searchResult'])
        # for i in range(len(result['searchResult']['item'])):
        #     print(result['searchResult']['item'][i]['title'])
        #     print(result['searchResult']['item'][i]['viewItemURL'])
        #     #print(result['searchResult']['item'][i]['productId'])
        # 
        #=======================================================================
        #finding_api.next_page()
        #response=finding_api.response
        #print(response.dict()['paginationOutput'])
        #response = finding_api.response_dict()
        #print 'here2'
        #print(finding_api.response_dict())
        #r=response.dict()
        
        #output = open('items_by_category.pkl', 'w')
        #pickle.dump(r,output,-1)
        #output.close()
        #r2=r['searchResult']
        #print(response)
    except ConnectionError as e:
        print(e)
        print(e.response.dict())
    



    #api = FindItem(config_file='ebay.yaml')
    #records = api.find_items_by_ids([121429757727])
    
    #print(records)
    
    #api = Connection(config_file='ebay.yaml', debug=False)
    #api = Connection(appid='ColinBau-c01c-4e3d-85cf-380674b047ac')
    #response = api.execute('findItemsAdvanced', {'keywords': 'legos'})
    
    #api = shopping(config_file='ebay.yaml',siteid=77)
    #response = api.execute('GetSingleItem', {'ItemID': '121429757727'})
    api = trading(config_file='ebay.yaml',siteid=77)
    response = api.execute('GetItem', {'ItemID': '111453544170'})
    print(response.dict())

#===============================================================================
#     assert(response.reply.ack == 'Success')
#     assert(type(response.reply.timestamp) == datetime.datetime)
#     assert(type(response.reply.searchResult.item) == list)
# 
#     item = response.reply.searchResult.item[0]
#     assert(type(item.listingInfo.endTime) == datetime.datetime)
#     assert(type(response.dict()) == dict)
#     assert(type(response.dom() == _Element))
#===============================================================================

computer=58058
notebooks=175672
apple_notebooks=111422
pc_notebooks=175672
tablets=171485
handys=15032
handys_ohne_vertrag=9355

def get_product_details(category_id):
    global shopping_api
    
    response = shopping_api.execute('FindProducts', {'CategoryID': category_id})
    print(response.dict())



if __name__ == '__main__':
    trading_api=None
    finding_api=None
    shopping_api=None
    connect_api()
    #get_categories()
    valid_aspect_categories=['Erscheinungsjahr','Prozessorgeschwindigkeit','Produktfamilie']
    category_aspects=get_category_aspects(apple_notebooks)
    category_aspects['Erscheinungsjahr']=['2012','2013','2014','Not Specified']
    category_order=[8,7,6]
    #print(category_aspects.keys())
    recursive_find(apple_notebooks,list(),0)
    #get_items_by_category(apple_notebooks,category_aspects,category_order)
    #get_items_by_category()
    #get_item_complete_description(251638689051)
    #get_product_details(apple_notebooks)
    #pkl_file = open('items_by_category.pkl', 'r')
    #data=pickle.load(pkl_file)
    #pkl_file.close()
    #print data['paginationOutput']
    #print data['searchResult']['item'][0]

    