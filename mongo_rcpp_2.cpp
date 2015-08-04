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
#include <set>
#include <iomanip>

using namespace Rcpp;
using mongo::BSONElement;
using mongo::BSONObj;
using mongo::BSONObjBuilder;

// g++ mongo_rcpp_2.cpp -pthread -lmongoclient -lboost_thread -lboost_system -lboost_regex -o mongo_rcpp_2


// Writes a data frame to a mongo db collection
// [[Rcpp::export]]
SEXP df_to_collection(DataFrame data_frame, CharacterVector collection){
	
	// Data frame related bits
	SEXP _column; // to store a column of anonymous type
	CharacterVector _names = data_frame.names(); // for the column names
	const char* _name; // for a column name for writing to database
	const char* _collection_name = collection[0];
	int nCols = data_frame.size();
	int nRows = data_frame.nrows();
	
	BSONObj _p;
	
	// Connection
	mongo::client::initialize();
	mongo::DBClientConnection _conn;
	try{
		_conn.connect("localhost");
	} catch(const mongo::DBException &_err){
			std::cout << "caught " << _err.what() << std::endl;
	}
	// Create a collection (table)
	_conn.createCollection(_collection_name);
	
	// This for loop runs over the rows and columns.
	for(int i = 0; i < nRows; ++i){
		//std::cout << "Row: " << i << std::endl;
		BSONObjBuilder _bM;
		for(int j = 0; j < nCols; ++j){
			_name = _names[j];
			_column = data_frame[_name];
			//std::cout << "Column: " << j << std::endl;	
			if(TYPEOF(_column) == REALSXP){
				NumericVector _field(_column);
				_bM.append(_name, _field[i]);
			} else if(TYPEOF(_column) == INTSXP){
				IntegerVector _field(_column);
				_bM.append(_name, _field[i]);
			} else if(TYPEOF(_column) == LGLSXP){
				IntegerVector _field(_column);
				_bM.append(_name, _field[i]);
			} else { // catch all convert to const char
				CharacterVector _field(_column);
				const char* _el;
				_el = _field[i];
				_bM.append(_name, _el);
			}
		}
		_p = _bM.obj();
		_conn.insert(_collection_name, _p);
	}
	
	return wrap(0);
}

//[[Rcpp::export]]
SEXP collection_to_df(CharacterVector collection_name){
	
	const char *_collection_name = collection_name[0];
	
	// Connection
	mongo::client::initialize();
	mongo::DBClientConnection _conn;
	try{
		_conn.connect("localhost");
	} catch(const mongo::DBException &_err){
			std::cout << "caught " << _err.what() << std::endl;
	}
	// Create a collection (table)
	_conn.createCollection(_collection_name);
	
	std::auto_ptr<mongo::DBClientCursor> _cursor = _conn.query(_collection_name, BSONObj());
	
	BSONObj _p;
	_p = _cursor->next();
	std::set<std::string> _field_names;
	int _nCol = _p.nFields();
	int _status = _p.getFieldNames(_field_names);
	std::string _name;
	const char *_nm;
	std::vector<const char*> _names;
	std::vector<std::string> _col_names;
	const char *_item;
	List _list;
	int n = 0;
	
	
	for(BSONObj::iterator it = _p.begin(); it.more(); ){
		BSONElement _bEl = it.next();
		_nm = _bEl.fieldName();
		_name = _nm;
		_col_names.push_back(_name);
		if(_bEl.isNumber()){
			NumericVector _column;
			_column.push_back(_bEl.number());
			_list[_name] = _column;
		}else{
			CharacterVector _column;
			_column.push_back(_bEl.str());
			_list[_name] = _column;
		}
	}
	
	while(_cursor->more()){
		_p = _cursor->next();
		for(BSONObj::iterator it = _p.begin(); it.more(); ){
			BSONElement _bEl = it.next();
			_nm = _bEl.fieldName();
			_name = _nm;
			//_col_names.push_back(_name);
			if(_bEl.isNumber()){
				NumericVector _column = _list[_name];
				_column.push_back(_bEl.number());
				_list[_name] = _column;
			}else{
				CharacterVector _column = _list[_name];
				_column.push_back(_bEl.str());
				_list[_name] = _column;
			}
		}
	}
	
	List _list_out;
	for(int i = 1; i < _col_names.size(); ++i){
		_list_out[_col_names[i]] = _list[_col_names[i]];
	}
	
	DataFrame _data_frame(_list_out);
	return wrap(_data_frame);
}

// [[Rcpp::export]]
SEXP delete_collection(CharacterVector collection_name){
	const char* _collection_name = collection_name[0];
	mongo::client::initialize();
	mongo::DBClientConnection _conn;
	try{
		_conn.connect("localhost");
	} catch(const mongo::DBException &_err){
			std::cout << "caught " << _err.what() << std::endl;
	}
	
	_conn.dropCollection(_collection_name);
	return wrap(0);
}


/*
# R code
require(Rcpp)
flgString <- '-pthread -lmongoclient -lboost_thread -lboost_system -lboost_regex'
Sys.setenv("PKG_CXXFLAGS"=flgString)
Sys.setenv("PKG_LIBS"=flgString)
sourceCpp("mongo_rcpp_2.cpp")
iris2 <- iris
iris2$Species <- as.character(iris2$Species)
df_to_collection(iris2[1:5,], "test.data")
collection_to_df("test.data")
delete_collection("test.data")
*/
