// Basic Connection to mongodb and write/readback of data
// then remove the "test.data" collection.

#include "mongo/client/dbclient.h" // for the driver
#ifdef Realloc
#undef Realloc
#endif
#include <R.h>
// [[Rcpp::interfaces(r, cpp)]]
#include <Rcpp.h>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <iomanip>

using namespace Rcpp;
using mongo::BSONElement;
using mongo::BSONObj;
using mongo::BSONObjBuilder;

// Function to convert character element to const char
const char* const_char(SEXP x){
	CharacterVector __x(x);
	const char * _x;
	_x = __x[0];
	return _x;
}

// [[Rcpp::export]]
SEXP testFun(NumericVector age, CharacterVector location){
	// Create the data
	std::vector<const char*> colNames;
	colNames.push_back("location");
	colNames.push_back("gender");
	colNames.push_back("age");
	colNames.push_back("name");
	colNames.push_back("salary");
	std::vector<const char*> __location(location.size());
	std::vector<const char*> gender;
	gender.push_back("male");
	gender.push_back("female");
	gender.push_back("male");
	std::vector<const char*> name;
	name.push_back("john");
	name.push_back("sandy");
	name.push_back("jean");
	std::vector<double> salary;
	salary.push_back(50000);
	salary.push_back(550000);
	salary.push_back(53000);
	
	// Initialization
	mongo::client::initialize();
	mongo::DBClientConnection c;
	try{
		c.connect("localhost");
	} catch(const mongo::DBException &e){
			std::cout << "caught " << e.what() << std::endl;
	}
	
	// Create a collection (table)
	c.createCollection("test.data");
	BSONObj p;
	const char *_location;
	for(int i=0; i < location.size(); ++i)
	{
		BSONObjBuilder b;
		_location = location[i];
		b.append(colNames[0], const_char(location[i]));
		b.append(colNames[1], gender[i]);
		b.append(colNames[2], age[i]);
		b.append(colNames[3], name[i]);
		b.append(colNames[4], salary[i]);
		p = b.obj();
		c.insert("test.data", p);
	}
	// Print out table
	std::auto_ptr<mongo::DBClientCursor> cursor = c.query("test.data", BSONObj());
	while(cursor->more()){
		p = cursor->next();
		std::cout << p["location"] << "\t" << p["gender"] << "\t\t" << "age: " << 
			p["age"].Number() << "\t" << p["name"] << "\t" << "salary: " << 
				p["salary"].Number() << std::endl;
	}
	
	// Cleaning up
	c.dropCollection("test.data");
	return wrap(0);
}

/*
# R code
require(Rcpp)
flgString <- '-pthread -lmongoclient -lboost_thread -lboost_system -lboost_regex'
Sys.setenv("PKG_CXXFLAGS"=flgString)
Sys.setenv("PKG_LIBS"=flgString)
sourceCpp("tutorial5_RTypes.cpp")
testFun(age = c(40, 55, 33), location = c("london", "new york", "paris"))
*/
