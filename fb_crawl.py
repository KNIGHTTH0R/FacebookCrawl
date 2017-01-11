# -*- coding: utf-8 -*-

import urllib2
import json
#import mysql.connector
import datetime
import webbrowser
import urllib
import re
import threading


def create_post_url(graph_url, access_token):
    #method to return 
    post_args = "/posts/?key=value&access_token=" + access_token
    post_url = graph_url + post_args
 
    return post_url
    
def create_comment_url(comment_id, access_token):
    #method to return 
    graph_url = "https://graph.facebook.com/v2.5/"    
    comment_args = "?fields=attachment,from,message,created_time,like_count,user_likes&access_token=" + access_token 
    comment_url = graph_url + comment_id + comment_args
 
    return comment_url

def render_to_json(graph_url):
    #render graph url call to JSON
    web_response = urllib2.urlopen(graph_url)
    readable_page = web_response.read()
    json_data = json.loads(readable_page)
    
    return json_data

def scrape_posts_by_date(graph_url, date, access_token, username,index):
    global np
    #render URL to JSON
    page = render_to_json(graph_url)
    next_page = page["paging"]["next"]
    
    np = graph_url
    #print np
    text_file=open('logs/log%d.txt' % index,"a")
    text_file.write("%s\n" % np)
    text_file.close()
    last_text_file=open('logs/last_log%d.txt' % index,"w")
    last_text_file.write("%s\n" % np)
    last_text_file.close()
    print "log saved"
    #grab all posts
    page_posts = page["data"]
        
    #boolean to tell us when to stop collecting
    collecting = True
    post_created_time = datetime.datetime.now().isoformat()
    
    #for each post capture data
    for post in page_posts:
        #post_type = post["type"]
        #if post_type == 'photo':        
        try:
            temp_post_id = post["id"]
            #print temp_post_id
            post_url = 'https://graph.facebook.com/v2.5/'+ temp_post_id + '?fields=from,shares,attachments,comments.limit(200),created_time,message,id&access_token=' + access_token
            post_data = render_to_json(post_url)
            post_type = post_data["attachments"]["data"][0]["type"]
            print post_type
            if post_type != 'photo':
                break
            if post_type == 'photo':
                post_id = post_data["id"]
                print post_id
                post_message = post_data["message"]
                post_message=post_message.replace("\n"," ")
                post_created_time = post_data["created_time"]
                post_created_time =post_created_time[:post_created_time.index('+')]
                f = open('logs/' + 'time%d.txt' % index,"a")
                f.write("%s\n" % str(post_created_time))
                f.close()
                post_like_count = get_likes_count(post_id, access_token)
                #post_like_count = get_likes_count(post["id"], access_token)           
                post_from = post_data["from"]["id"]           
                post_share_count =  post_data["shares"]["count"]
                post_picture_url = post_data["attachments"]["data"][0]["media"]["image"]["src"]
                r = urllib2.urlopen(post_picture_url)
                f = open('images/' +  'p' + post_id + '.jpg', 'wb')
                f.write(r.read())
                f.close()
                post_picture = 'Facebook_data\\\\' + username + '\\\\' + 'p' + str(post_id) + '.jpg'
                post_comments = post_data["comments"]["data"]
                post_comment_counts = len(post_comments)             
                            
                for comment in post_comments:
                    try:
                        comment_url = create_comment_url(comment["id"] , access_token)
                        comment_data = render_to_json(comment_url)
                        comment_id = comment_data["id"]
                        print 'comment' + comment_id
                        comment_from = comment_data["from"]["id"]                    
                        comment_message = comment_data["message"]
                        comment_message=comment_message.replace("\n"," ")
                        comment_created_time = comment_data["created_time"]
                        comment_created_time =comment_created_time[:comment_created_time.index('+')]
                        comment_likes_count = comment_data["like_count"]
                        comment_user_likes = comment_data["user_likes"]
                        #print 'Yes'
                        try:                    
                            #comment_url = create_comment_url(comment_id , access_token)
                            #comments_data = render_to_json(comment_url)
                            comment_picture_url = comment_data["attachment"]["media"]["image"]["src"]
                            r = urllib2.urlopen(comment_picture_url)
                            f1 = open('images/' + 'p' + post_id + 'c' + comment_id + '.jpg', 'wb')
                            f1.write(r.read())
                            f1.close()              
                            #print comment_picture_url
                            comment_picture = 'Facebook_data\\\\' + username + '\\\\' + 'p' + str(post_id) + 'c' + str(comment_id) + '.jpg'                     
                            #urllib.urlretrieve(comment_picture_url, 'E:\Facebook_data\\' + username + post_id + '_' + comment_id + '.jpg')
                            #print 'before insert comment'
                            comment_file = open('data/comment%d.txt' % index,"a")
                            comment_file.write("%s, %s, %s, %s, %r, %s, %s, %s\n" % (comment_id,post_id,comment_likes_count, comment_from,comment_message,comment_user_likes,comment_picture,comment_created_time))
                            comment_file.close()                            
                            print "comment saved"
                            '''insert_comments = "INSERT INTO COMMENT (COMMENT_ID,POST_ID, LIKE_COUNT,FROM_ID ,MESSAGE,USER_LIKE, PICTURE_PATHNAME,CREATED_TIME)\
                            VALUES ('%s','%s',%d,'%s','%s','%s','%s',STR_TO_DATE('%s','%%Y-%%m-%%dT%%H:%%i:%%s'))" % \
                            (comment_id,post_id,comment_likes_count, comment_from,comment_message,comment_user_likes,comment_picture,comment_created_time)
                            #print 'insert_comments'
                            
                            connection = connect_db()
                            cursor = connection.cursor()                 
                            try: 
                                cursor.execute(insert_comments)
                                connection.commit()
                                connection.close()   
                            
                            except mysql.connector.IntegrityError as err:
                                print("Error: {}".format(err))  '''
                        
                        
                        except Exception:
                                #print "error in Comment image"
                                comment_picture = "NA"
                                comment_file = open('data/comment%d.txt' % index,"a")
                                comment_file.write("%s, %s, %s, %s, %s, %s, %s, %s\n" % (comment_id,post_id,comment_likes_count, comment_from,comment_message,comment_user_likes,comment_picture,comment_created_time))
                                comment_file.close()
                                print "comment saved"
                
                    except Exception: 
                        print "other error in comment"
                        None
                    
        except Exception:
                #print "Error in Post"
                post_created_time="error"
            
        if post_created_time != "error":
                #print date
                #print post_created_time
                if date <= post_created_time:
                    #SQL statement for adding Facebook page data to database
                    post_file = open('data/post%d.txt' % index,"a")
                    post_file.write("%s, %s, %s, %r, %s, %s, %s, %s\n" % (post_id,post_like_count,post_from,post_message,post_picture,post_comment_counts,post_created_time,post_share_count))
                    post_file.close()
                    print "post saved"
                    '''insert_posts = "INSERT INTO POST \
                        (POST_ID,LIKE_COUNT,FROM_ID,MESSAGE,PICTURE_PATHNAME,COMMENT_COUNT,CREATED_TIME,SHARE_COUNT)\
                        VALUES ('%s',%d,'%s','%s','%s',%d,STR_TO_DATE('%s','%%Y-%%m-%%dT%%H:%%i:%%s'),%d)" % \
                        (post_id,post_like_count,post_from,post_message,post_picture,post_comment_counts,post_created_time,post_share_count)
                    
                    
                    connection = connect_db()
                    cursor = connection.cursor()                 
                    try:
                        cursor.execute(insert_posts)
                        connection.commit()
                        connection.close()   
                    except mysql.connector.IntegrityError as err:
                        print("Error: {}".format(err))  '''     
                    
            
            
                elif date > post_created_time:
                    print "Done collecting"
                    collecting = False
                    break
    
    
    #If we still don't meet date requirements, run on next page            
    if collecting == True:
        #extract next page
        scrape_posts_by_date(next_page, date, access_token, username,index)
      
    
        
def get_likes_count(post_id, access_token):
    #create Graph API Call
    graph_url = "https://graph.facebook.com/v2.5/" 
    likes_args = post_id + "/likes?summary=true&key=value&access_token=" + access_token
    likes_url = graph_url + likes_args
    likes_json = render_to_json(likes_url)
 
    #pick out the likes count
    count_likes = likes_json["summary"]["total_count"]
 
    return count_likes
    

'''def connect_db():
    #fill this out with your db connection info
    connection = mysql.connector.connect(user='root', password='facebook',
                                         host = '127.0.0.1',
                                         database='test',use_unicode=True,charset='utf8')
    return connection'''

def crawl(access_token,index):
    print('thread %d started' % index)    
    #simple data pull App Secret and App ID
    #APP_SECRET = "2a925676f928ffa61c7ae11c14bad47d"
    #APP_ID = "812263125567599"
    
    #to find go to page's FB page, at the end of URL find username
    #e.g. http://facebook.com/walmart, walmart is the username
    list_companies = ["internationalchaluunion"]
    graph_url = "https://graph.facebook.com/v2.5/"
    
    #the time of last weeks crawl
    last_crawl = datetime.datetime.now() - datetime.timedelta(weeks=250)
    last_crawl = last_crawl.isoformat()
  
    company="internationalchaluunion"
    #make graph api url with company username
    #open public page in facebook graph api
    
 
    #gather our page level JSON Data
    
    #after refreshing access token
    #post_url = 'https://graph.facebook.com/v2.5/632474226810626/posts?limit=25&__paging_token=enc_AdBw7ITgoJMBm2ZCZChllfTKc9oRlhSBGsDUS9eZBFoxaOHxm6ZAWZAbGahAwx9ngIyBFt8bHgw8GHe1VG45HZApgMcOZB3XZBnl0AOAiZCFgze687ncENgZDZD&access_token=CAACEdEose0cBAJVhgB0lylZA0FJBBcyl7h7crCKtynizRwGZBuivdBHhRogSRr8VPqonohZANgkpaUaNuU5cRHZCqvio8O6DLjYTbZCt6cjxkcww7a4LMd0vLAUZCkoEqG4jfS0EztQnMVIdjJl6cUSOIXeF5Hi7RJ6d6bX0NWPZBVwHVqrqfuZCOrBZACw7yIOZA50Rg055LYxnE6FPtHiYZBv&until=1456066857'
    last_text_file=open('logs/last_log%d.txt' % index,"r")
    post_url=last_text_file.readline()
    last_text_file.close()    
    url=post_url.split('&access_token=')[0]
    url2=post_url.split('&access_token=')[1].split('&')[1]
    post_url=url + '&access_token=' + access_token + '&' + url2
    scrape_posts_by_date(post_url, last_crawl, access_token,company,index)
    

if __name__ == "__main__":
        access_token_list = ['EAACEdEose0cBAI2kCPwASuX7IcflZBSvEL5juGZCSgLaGhpLcIBjc5R4dM3ZAKMrOre5zXEgrcLSwsmkoOLZAa6mZAET3ZAalgi0q8ypEeupeJ1Bt3sW9atZCTtPya7aCHYBUjTIcvwVhrYNZCGN6QnXE8m95RlrcEZC4jYFpwUiXEAZDZD', 'EAACEdEose0cBAKHriN4zeaRMa6s3u2fa6ZC6NGW3PMM0hkzM2ruDXO4JhH4tBwu9KsQ5eA0IXdhhhMz95aVoHNZCtIMGtn0nh79fXsVHKKO6gHrfO9ldBfqdkwGczh6HveVdz5SuWBz1Q0STKzTgZCH24dGnxg9Bjx5MZANvggZDZD', 'EAACEdEose0cBAGLjOyktnl01C9QjNW4HeqStsk75172fQ79aZAOdDxHza5cJMafRDUzJSFiASw0lZC9J71ulmOVf9fcN2ZChepzvKpwLCmiPdxW4SU3vam5ii2975ZBcQKYokuh8pkaMqL0ERT7IhqMTK7MZC3JBliNMGy4RC3gZDZD','EAACEdEose0cBAGJX4PgJJCF3aZACmSVIKVb4jwhV6hHJv703FmUrxfBsKS83TuK60ZCSqnI3iiMQtPHDeaTFIqn2agJvp3gs8FKLNjj6Qu5GOSLW2fmifpxoXr8MnVi4WZBZBIIwBrPOJOAl8oo4W5D45iwvHplDhTzedZBMOEAZDZD','EAACEdEose0cBAPdTbZAjSlNlXGT6evAUUrcuZBY3m4OZAdSjGMb8uQgUp2mpSIewrV7eEGz1Uw6eDknCLH3jYrmxVxDtsbeSZC9Cpt4PvcwTKaZBh2Dcsg6bCbgeqA4SVAy8w9Ntv661weLWfOJRIBgYKoCg3oRWXrCdTeI85KQZDZD','EAACEdEose0cBAIqOsB6XK2zOkNYyrqFGySSFbgucmo8nItzdvQTZAwurEuwYYDLtd6UlzzE2sMQ3EiCsrNJvf8KRmdMb8xVgwpjZC4sWpDM4WXoAieTZAjF42FeIuzVZBTF2HV9WS3jyGZCrTIMwiVJdv8nIZARaMZAevvjULdLpQZDZD']
        for i in range(len(access_token_list)):
            t=threading.Thread(target=crawl,args=(access_token_list[i],i))
            t.start()
            
        
