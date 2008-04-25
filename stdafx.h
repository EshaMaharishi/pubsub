// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//

#pragma once

#if defined(_WIN32)
const bool debug=true;
#else
const bool debug=false;
#endif

#include "targetver.h"

//#include "assert.h"

// you can catch these
class AssertionException { 
public:
	AssertionException() { }
};

void asserted(const char *msg, const char *file, unsigned line);
void wasserted(const char *msg, const char *file, unsigned line);

#ifdef assert
#undef assert
#endif

#define assert(_Expression) (void)( (!!(_Expression)) || (asserted(#_Expression, __FILE__, __LINE__), 0) )

#define xassert(_Expression) (void)( (!!(_Expression)) || (asserted(#_Expression, __FILE__, __LINE__), 0) )

#define yassert 1

/* warning only - keeps going */
#define wassert(_Expression) (void)( (!!(_Expression)) || (wasserted(#_Expression, __FILE__, __LINE__), 0) )

#include <stdio.h>
#include <sstream>
#include <signal.h>

typedef char _TCHAR;

#include <iostream>
#include <fstream>
using namespace std;

#include "time.h"
#include <map>
#include <string>
#include <vector>
#include <set>

#if !defined(_WIN32)
typedef int HANDLE;
inline void strcpy_s(char *dst, unsigned len, const char *src) { strcpy(dst, src); }
#else
typedef void *HANDLE;
#endif

//#if defined(CHAR)
//#error CHAR already defined?
//#endif

//#if defined(_WIN32_WINNT)
//typedef wchar_t CHAR;
//#else
// more to be done...linux unicode is 32 bit.
//typedef unsigned short CHAR; // 16 bit unicode
//#endif

#define null (0)

#include <vector>

// for debugging
typedef struct _Ints { int i[100]; } *Ints;
typedef struct _Chars { char c[200]; } *Chars;

typedef char CHARS[400];

typedef struct _OWS {
	int size;
	char type;
	char string[400];
} *OWS;

extern ofstream problems;

// not threadsafe
inline ofstream& problem() { 
	time_t t;
	time(&t);
	string now(ctime(&t),0,20);
	problems << now;
	return problems;
}
