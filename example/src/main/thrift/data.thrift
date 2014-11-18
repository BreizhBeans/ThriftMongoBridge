namespace java org.breizhbeans.thrift.tools.thriftmongobridge.example

/**
* Very simple data structure to store
*
* firstName, lastName, birthday and email are very important fields
**/
struct People {
	1:string firstName,
	2:string lastName,
	3:string birthday
	4:string email,
	5:string language,
	6:i64 inceptionDate,
}
