#-*- encoding:utf-8 -*-
import urllib2
import re
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class JD_spider:
    def __init__(self,page):
        self.page = page
        self.url_head = "http://club.jd.com/review/1041267874-0-"
        self.url_tail = "-0.html"
    def get_max_page(self):
        url = self.url_head + str(self.page) + self.url_tail
        page_context = urllib2.urlopen(url).read()
        myMatch = re.search(r'(\d+?)</a><a href=http://club.jd.com/review/1041267874-0-2-0.html>下一页</a>',page_context,re.S)
        if myMatch:
            max_page = int(myMatch.group(1))
        else:
            max_page = 1
        return max_page

    def get_comment(self,max_page):
        total_page = max_page
        file = open('comment.txt','a+')
        i = 1
        for page in range(self.page,total_page+1):
            print '\n第'+str(page)+'页的评论如下：\n'
            url = self.url_head + str(page) + self.url_tail
            page_context = urllib2.urlopen(url).read()
            #print page_context
            myItems = re.findall(r'得：</dt>\s+<dd>(.*?)</dd>',page_context,re.S)
            for item in myItems:
                item = item.replace('\n','')
                print item
                file.write(str(i)+':'+item+'\n')
                i += 1
        file.close()

def main():
        get_JD_comment = JD_spider(1)
        print '@Author: thoms.zhang'
        max_page = get_JD_comment.get_max_page()
        get_JD_comment.get_comment(max_page)
if __name__ == '__main__':
    main()