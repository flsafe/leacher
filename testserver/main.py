import sys

from twisted.internet import reactor, endpoints
from twisted.news.news import UsenetServerFactory
from twisted.news.nntp import NNTPServer
from twisted.news.database import NewsShelf

def main(args):
    shelf = NewsShelf("", "./storage")
    shelf.addGroup("alt.binaries.test", '')
    
    factory = UsenetServerFactory(shelf)
    endpoints.serverFromString(reactor, "tcp:5000").listen(factory)
    reactor.run()

if __name__ == "__main__":
    main(sys.argv)
