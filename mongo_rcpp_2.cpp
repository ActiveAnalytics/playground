// Attempting to write basic data frame iris into mongodb via Rcpp
// Uses the legacy 1.0.0 mongodb cxx driver
#include "mongo/client/dbclient.h" // for the driver
#ifdef Realloc
#undef Realloc
#endif
// Above answered on stack overflow to avoid Realloc conflicts:
// http://stackoverflow.com/questions/31766585/rcpp-and-mongodb-c-driver-compilation-error-realloc-conflict
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

// g++ mongo_rcpp_2.cpp -pthread -lmongoclient -lboost_thread -lboost_system -lboost_regex -o mongo_rcpp_2

// Function to check the sexp type
// [[Rcpp::export]]
SEXP check_type(SEXP x){
	if(TYPEOF(x) == STRSXP){
		const char* _print = CHAR(Rf_asChar(x));
		std::cout << _print << " is a string" << std::endl;
	}else{
		std::cout << "This is not a string!" << std::endl;
	}
	return wrap(0);
}

// Creates BSONObj with single BSONElement and returns it
BSONObj sexp_to_bson(SEXP field_name, SEXP field_value){
	const char* _field_name = CHAR(Rf_asChar(field_name));
	BSONObj _p;
	BSONObjBuilder b;
	if(TYPEOF(field_value) == REALSXP){
			double numeric_field = Rf_asReal(field_value);
			b.append(_field_name, numeric_field);
	} else if(TYPEOF(field_value) == INTSXP){
			int int_field = Rf_asInteger(field_value);
			b.append(_field_name, int_field);
	} else if(TYPEOF(field_value) == LGLSXP){
			int int_field = Rf_asLogical(field_value);
			b.append(_field_name, int_field);
	} else { // catch all convert to const char
			const char* char_field = CHAR(Rf_asChar(field_value));
			b.append(_field_name, char_field);
	}
	_p = b.obj();
	return(_p);
}

BSONObj sexp_to_bson_obj(SEXP field_name, SEXP field_value){
	const char* _field_name = CHAR(Rf_asChar(field_name));
	BSONObj _p;
	BSONObjBuilder b;
	if(TYPEOF(field_value) == REALSXP){
		NumericVector _field(field_value);
		for(int i = 0; i < _field.size(); ++i){
			b.append(_field_name, _field[i]);
		}
	} else if(TYPEOF(field_value) == INTSXP){
		IntegerVector _field(field_value);
		for(int i = 0; i < _field.size(); ++i){
			b.append(_field_name, _field[i]);
		}
	} else if(TYPEOF(field_value) == LGLSXP){
		IntegerVector _field(field_value);
		for(int i = 0; i < _field.size(); ++i){
			b.append(_field_name, _field[i]);
		}
	} else { // catch all convert to const char
		CharacterVector _field(field_value);
		const char* _el;
		for(int i = 0; i < _field.size(); ++i){
			_el = _field[i];
			b.append(_field_name, _el);
		}
	}
	_p = b.obj();
	return(_p);
}

// Overload
BSONObj sexp_to_bson_obj(const char* _field_name, SEXP field_value){
	BSONObj _p;
	BSONObjBuilder b;
	if(TYPEOF(field_value) == REALSXP){
		NumericVector _field(field_value);
		for(int i = 0; i < _field.size(); ++i){
			b.append(_field_name, _field[i]);
		}
	} else if(TYPEOF(field_value) == INTSXP){
		IntegerVector _field(field_value);
		for(int i = 0; i < _field.size(); ++i){
			b.append(_field_name, _field[i]);
		}
	} else if(TYPEOF(field_value) == LGLSXP){
		IntegerVector _field(field_value);
		for(int i = 0; i < _field.size(); ++i){
			b.append(_field_name, _field[i]);
		}
	} else { // catch all convert to const char
		CharacterVector _field(field_value);
		const char* _el;
		for(int i = 0; i < _field.size(); ++i){
			_el = _field[i];
			b.append(_field_name, _el);
		}
	}
	_p = b.obj();
	return(_p);
}


// Test function
BSONObj bson_out(){
	BSONObj p;
	BSONObjBuilder b;
	b.append("foo", "bar");
	p = b.obj();
	return p;
}

// Test BSON Output (string data only)
// [[Rcpp::export]]
SEXP test_bson(SEXP field_name, SEXP field_values){
	BSONObj _p = sexp_to_bson_obj(field_name, field_values);
	std::vector<BSONElement> _v;
	_p.elems(_v);
	std::string _val;
	for(int i = 0; i < _v.size(); ++i){
	_v[i].Val(_val);
	std::cout << "name: " << _v[i].fieldName() << ", value: "<< _val << std::endl;
	}
	return wrap(0);
}


// [[Rcpp::export]]
SEXP iter_df(DataFrame df){
	SEXP _col;
	CharacterVector _names = df.names();
	const char* _name;
	std::vector<BSONElement> _v;
	std::vector<std::vector<BSONElement> > _v_vec;
	BSONObj _p;
	// Convert the DF to BSONObj Array
	for(int i = 0; i < df.length(); ++i){
		_name = _names[i];
		_col = df[_name];
		for(int j = 0; j < df.nrows(); ++j){
			_p = sexp_to_bson_obj(_name, _col);
			_p.elems(_v);
			_v_vec.push_back(_v);
		}
	}
	std::cout << "point 1" << std::endl;
	// Connection
	mongo::client::initialize();
	mongo::DBClientConnection conn;
	try{
		conn.connect("localhost");
	} catch(const mongo::DBException &err){
			std::cout << "caught " << err.what() << std::endl;
	}
	// Create a collection (table)
	conn.createCollection("test.data");
	BSONElement _el;
	BSONObjBuilder _b;
	for(int m = 0; m < _v_vec[0].size(); ++m){ // rows
		for(int n = 0; n < _v_vec.size(); ++n){ // columns
			std::cout << "point 2" << std::endl;
			_el = _v_vec[m][n];
			_b.append(_el); // issue
		}
		conn.insert("test.data", _b.obj());
	}
	// Print out table
	std::auto_ptr<mongo::DBClientCursor> cursor = conn.query("test.data", BSONObj());
	while(cursor->more()){
		_p = cursor->next();
		std::cout << "Sepal.Length: " << _p["Sepal.Length"].Number() << "\t" << 
		", Sepal.Width: " << _p["Sepal.Width"].Number() << "\t" << ", Petal.Length: " << 
			_p["Petal.Length"].Number() << "\t" << ", Petal.Width: " << 
			_p["Petal.Width"].Number() << "\t" << _p["Species"] << std::endl;
	}
	
	// Cleaning up
	conn.dropCollection("test.data");
	
	return wrap(0);
}


/*
// [[Rcpp::export]]
SEXP iter_df(DataFrame df){
	SEXP _col;
	CharacterVector _names = df.names();
	const char* _name;
	std::vector<BSONObj> _bv;
	// Convert the DF to BSONObj Array
	for(int i = 0; i < df.length(); ++i){
		_name = _names[i];
		_col = df[_name];
		for(int j = 0; j < df.nrows(); ++j){
			_bv.push_back(sexp_to_bson_obj(_name, _col));
		}
	}
	
	// Connection
	mongo::client::initialize();
	mongo::DBClientConnection conn;
	try{
		conn.connect("localhost");
	} catch(const mongo::DBException &err){
			std::cout << "caught " << err.what() << std::endl;
	}
	// Create a collection (table)
	conn.createCollection("test.data");
	BSONObj _p;
	BSONElement _el;
	BSONObjBuilder _b;
	for(int m = 0; m < _bv[0].nFields(); ++m){
		for(int n = 0; n < _bv.size(); ++n){
			//_p = _bv[n];
			//_el = _bv[n][m];
			_b.append(_bv[n][m]); // issue
		}
		conn.insert("test.data", _b.obj());
	}
	// Print out table
	std::auto_ptr<mongo::DBClientCursor> cursor = conn.query("test.data", BSONObj());
	while(cursor->more()){
		_p = cursor->next();
		std::cout << "Sepal.Length: " << _p["Sepal.Length"].Number() << "\t" << 
		", Sepal.Width: " << _p["Sepal.Width"].Number() << "\t" << ", Petal.Length: " << 
			_p["Petal.Length"].Number() << "\t" << ", Petal.Width: " << 
			_p["Petal.Width"].Number() << "\t" << _p["Species"] << std::endl;
	}
	
	// Cleaning up
	conn.dropCollection("test.data");
	
	return wrap(0);
}
*/

// [[Rcpp::export]]
SEXP test(){
	CharacterVector x(1);
	x[0] = "HW!";
	return x;
}

/*
# R code
# This is currently broken
require(Rcpp)
flgString <- '-pthread -lmongoclient -lboost_thread -lboost_system -lboost_regex'
Sys.setenv("PKG_CXXFLAGS"=flgString)
Sys.setenv("PKG_LIBS"=flgString)
sourceCpp("mongo_rcpp_2.cpp")
iris2 <- iris
iris2$Species <- as.character(iris2$Species)
iter_df(iris2)
*/
