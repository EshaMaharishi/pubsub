// indexdetailstests.cpp : IndexDetails unit tests.
//

/**
 *    Copyright (C) 2008 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

// Where IndexDetails defined.
#include "../db/namespace.h"

#include "../db/db.h"
#include "../db/json.h"

#include "dbtests.h"

namespace NamespaceTests {
namespace IndexDetailsTests {
class Base {
public:
    Base() {
        dblock lk;
        setClient( ns() );
    }
    ~Base() {
        if ( id_.info.isNull() )
            return;
        theDataFileMgr.deleteRecord( ns(), id_.info.rec(), id_.info );
        ASSERT( theDataFileMgr.findAll( ns() )->eof() );
    }
protected:
    void create() {
        BSONObjBuilder builder;
        builder.append( "ns", ns() );
        builder.append( "name", "testIndex" );
        builder.append( "key", key() );
        BSONObj bobj = builder.done();
        id_.info = theDataFileMgr.insert( ns(), bobj.objdata(), bobj.objsize() );
        // head not needed for current tests
        // idx_.head = BtreeBucket::addHead( id_ );
    }
    static const char* ns() {
        return "sys.unittest.indexdetailstests";
    }
    const IndexDetails& id() {
        return id_;
    }
    virtual BSONObj key() const {
        BSONObjBuilder k;
        k.append( "a", 1 );
        return k.doneAndDecouple();
    }
    BSONObj aDotB() const {
        BSONObjBuilder k;
        k.append( "a.b", 1 );
        return k.doneAndDecouple();
    }
    BSONObj aAndB() const {
        BSONObjBuilder k;
        k.append( "a", 1 );
        k.append( "b", 1 );
        return k.doneAndDecouple();
    }
    static vector< int > shortArray() {
        vector< int > a;
        a.push_back( 1 );
        a.push_back( 2 );
        a.push_back( 3 );
        return a;
    }
    static BSONObj simpleBC( int i ) {
        BSONObjBuilder b;
        b.append( "b", i );
        b.append( "c", 4 );
        return b.doneAndDecouple();
    }
    static void checkSize( int expected, const set< BSONObj >  &objs ) {
        ASSERT_EQUALS( set< BSONObj >::size_type( expected ), objs.size() );
    }
private:
    IndexDetails id_;
};

class Create : public Base {
public:
    void run() {
        create();
        ASSERT_EQUALS( "testIndex", id().indexName() );
        ASSERT_EQUALS( ns(), id().parentNS() );
        // check equal
        ASSERT_EQUALS( key(), id().keyPattern() );
    }
};

class GetKeysFromObjectSimple : public Base {
public:
    void run() {
        create();
        BSONObjBuilder b, e;
        b.append( "b", 4 );
        b.append( "a", 5 );
        e.append( "", 5 );
        set< BSONObj > keys;
        id().getKeysFromObject( b.done(), keys );
        checkSize( 1, keys );
        ASSERT_EQUALS( e.doneAndDecouple(), *keys.begin() );
    }
};

class GetKeysFromObjectDotted : public Base {
public:
    void run() {
        create();
        BSONObjBuilder a, e, b;
        b.append( "b", 4 );
        a.append( "a", b.done() );
        a.append( "c", "foo" );
        e.append( "", 4 );
        set< BSONObj > keys;
        id().getKeysFromObject( a.done(), keys );
        checkSize( 1, keys );
        ASSERT_EQUALS( e.doneAndDecouple(), *keys.begin() );
    }
private:
    virtual BSONObj key() const {
        return aDotB();
    }
};

class GetKeysFromArraySimple : public Base {
public:
    void run() {
        create();
        BSONObjBuilder b;
        b.append( "a", shortArray()) ;

        set< BSONObj > keys;
        id().getKeysFromObject( b.done(), keys );
        checkSize( 3, keys );
        int j = 1;
        for ( set< BSONObj >::iterator i = keys.begin(); i != keys.end(); ++i, ++j ) {
            BSONObjBuilder b;
            b.append( "", j );
            ASSERT_EQUALS( b.doneAndDecouple(), *i );
        }
    }
};

class GetKeysFromArrayFirstElement : public Base {
public:
    void run() {
        create();
        BSONObjBuilder b;
        b.append( "a", shortArray() );
        b.append( "b", 2 );

        set< BSONObj > keys;
        id().getKeysFromObject( b.done(), keys );
        checkSize( 3, keys );
        int j = 1;
        for ( set< BSONObj >::iterator i = keys.begin(); i != keys.end(); ++i, ++j ) {
            BSONObjBuilder b;
            b.append( "", j );
            b.append( "", 2 );
            ASSERT_EQUALS( b.doneAndDecouple(), *i );
        }
    }
private:
    virtual BSONObj key() const {
        return aAndB();
    }
};

class GetKeysFromArraySecondElement : public Base {
public:
    void run() {
        create();
        BSONObjBuilder b;
        b.append( "first", 5 );
        b.append( "a", shortArray()) ;

        set< BSONObj > keys;
        id().getKeysFromObject( b.done(), keys );
        checkSize( 3, keys );
        int j = 1;
        for ( set< BSONObj >::iterator i = keys.begin(); i != keys.end(); ++i, ++j ) {
            BSONObjBuilder b;
            b.append( "", 5 );
            b.append( "", j );
            ASSERT_EQUALS( b.doneAndDecouple(), *i );
        }
    }
private:
    virtual BSONObj key() const {
        BSONObjBuilder k;
        k.append( "first", 1 );
        k.append( "a", 1 );
        return k.doneAndDecouple();
    }
};

class GetKeysFromSecondLevelArray : public Base {
public:
    void run() {
        create();
        BSONObjBuilder b;
        b.append( "b", shortArray() );
        BSONObjBuilder a;
        a.append( "a", b.done() );

        set< BSONObj > keys;
        id().getKeysFromObject( a.done(), keys );
        checkSize( 3, keys );
        int j = 1;
        for ( set< BSONObj >::iterator i = keys.begin(); i != keys.end(); ++i, ++j ) {
            BSONObjBuilder b;
            b.append( "", j );
            ASSERT_EQUALS( b.doneAndDecouple(), *i );
        }
    }
private:
    virtual BSONObj key() const {
        return aDotB();
    }
};

class ParallelArraysBasic : public Base {
public:
    void run() {
        create();
        BSONObjBuilder b;
        b.append( "a", shortArray() );
        b.append( "b", shortArray() );

        set< BSONObj > keys;
        ASSERT_EXCEPTION( id().getKeysFromObject( b.done(), keys ),
                          UserAssertionException );
    }
private:
    virtual BSONObj key() const {
        return aAndB();
    }
};

class ArraySubobjectBasic : public Base {
public:
    void run() {
        create();
        vector< BSONObj > elts;
        for ( int i = 1; i < 4; ++i )
            elts.push_back( simpleBC( i ) );
        BSONObjBuilder b;
        b.append( "a", elts );

        set< BSONObj > keys;
        id().getKeysFromObject( b.done(), keys );
        checkSize( 3, keys );
        int j = 1;
        for ( set< BSONObj >::iterator i = keys.begin(); i != keys.end(); ++i, ++j ) {
            BSONObjBuilder b;
            b.append( "", j );
            ASSERT_EQUALS( b.doneAndDecouple(), *i );
        }
    }
private:
    virtual BSONObj key() const {
        return aDotB();
    }
};

class ArraySubobjectMultiFieldIndex : public Base {
public:
    void run() {
        create();
        vector< BSONObj > elts;
        for ( int i = 1; i < 4; ++i )
            elts.push_back( simpleBC( i ) );
        BSONObjBuilder b;
        b.append( "a", elts );
        b.append( "d", 99 );

        set< BSONObj > keys;
        id().getKeysFromObject( b.done(), keys );
        checkSize( 3, keys );
        int j = 1;
        for ( set< BSONObj >::iterator i = keys.begin(); i != keys.end(); ++i, ++j ) {
            BSONObjBuilder c;
            c.append( "", j );
            c.append( "", 99 );
            ASSERT_EQUALS( c.doneAndDecouple(), *i );
        }
    }
private:
    virtual BSONObj key() const {
        BSONObjBuilder k;
        k.append( "a.b", 1 );
        k.append( "d", 1 );
        return k.doneAndDecouple();
    }
};

class ArraySubobjectSingleMissing : public Base {
public:
    void run() {
        create();
        vector< BSONObj > elts;
        BSONObjBuilder s;
        s.append( "foo", 41 );
        elts.push_back( s.doneAndDecouple() );
        for ( int i = 1; i < 4; ++i )
            elts.push_back( simpleBC( i ) );
        BSONObjBuilder b;
        b.append( "a", elts );

        set< BSONObj > keys;
        id().getKeysFromObject( b.done(), keys );
        checkSize( 3, keys );
        int j = 1;
        for ( set< BSONObj >::iterator i = keys.begin(); i != keys.end(); ++i, ++j ) {
            BSONObjBuilder b;
            b.append( "", j );
            ASSERT_EQUALS( b.doneAndDecouple(), *i );
        }
    }
private:
    virtual BSONObj key() const {
        return aDotB();
    }
};

class ArraySubobjectMissing : public Base {
public:
    void run() {
        create();
        vector< BSONObj > elts;
        BSONObjBuilder s;
        s.append( "foo", 41 );
        for ( int i = 1; i < 4; ++i )
            elts.push_back( s.done() );
        BSONObjBuilder b;
        b.append( "a", elts );

        set< BSONObj > keys;
        id().getKeysFromObject( b.done(), keys );
        checkSize( 0, keys );
    }
private:
    virtual BSONObj key() const {
        return aDotB();
    }
};
} // namespace IndexDetailsTests

// TODO
// array subelement complex
// parallel arrays complex
// allowed multi array indexes

class All : public UnitTest::Suite {
public:
    All() {
        add< IndexDetailsTests::Create >();
        add< IndexDetailsTests::GetKeysFromObjectSimple >();
        add< IndexDetailsTests::GetKeysFromObjectDotted >();
        add< IndexDetailsTests::GetKeysFromArraySimple >();
        add< IndexDetailsTests::GetKeysFromArrayFirstElement >();
        add< IndexDetailsTests::GetKeysFromArraySecondElement >();
        add< IndexDetailsTests::GetKeysFromSecondLevelArray >();
        add< IndexDetailsTests::ParallelArraysBasic >();
        add< IndexDetailsTests::ArraySubobjectBasic >();
        add< IndexDetailsTests::ArraySubobjectMultiFieldIndex >();
        add< IndexDetailsTests::ArraySubobjectSingleMissing >();
        add< IndexDetailsTests::ArraySubobjectMissing >();
    }
};
}

UnitTest::TestPtr namespaceTests() {
    return UnitTest::createSuite< NamespaceTests::All >();
}
