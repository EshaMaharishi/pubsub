/**
*    Copyright (C) 2011 10gen Inc.
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

#include "pch.h"

#include "db/pipeline/document_source.h"

#include "db/jsobj.h"
#include "db/pipeline/expression.h"
#include "db/pipeline/value.h"

namespace mongo {

    const char DocumentSourceFilter::filterName[] = "$filter";

    DocumentSourceFilter::~DocumentSourceFilter() {
    }

    bool DocumentSourceFilter::coalesce(
	const shared_ptr<DocumentSource> &pNextSource) {

	/* we only know how to coalesce other filters */
	DocumentSourceFilter *pDocFilter =
	    dynamic_cast<DocumentSourceFilter *>(pNextSource.get());
	if (!pDocFilter)
	    return false;

	/*
	  Two adjacent filters can be combined by creating a conjunction of
	  their predicates.
	 */
	shared_ptr<ExpressionNary> pAnd(ExpressionAnd::create());
	pAnd->addOperand(pFilter);
	pAnd->addOperand(pDocFilter->pFilter);
	pFilter = pAnd;

	return true;
    }

    void DocumentSourceFilter::optimize() {
	pFilter = pFilter->optimize();
    }

    void DocumentSourceFilter::sourceToBson(BSONObjBuilder *pBuilder) const {
	pFilter->addToBsonObj(pBuilder, filterName, 0);
    }

    bool DocumentSourceFilter::accept(
	const intrusive_ptr<Document> &pDocument) const {
	intrusive_ptr<const Value> pValue(pFilter->evaluate(pDocument));
	return pValue->coerceToBool();
    }

    shared_ptr<DocumentSource> DocumentSourceFilter::createFromBson(
	BSONElement *pBsonElement,
	const intrusive_ptr<ExpressionContext> &pCtx) {
        assert(pBsonElement->type() == Object);
        // CW TODO error: expression object must be an object

	Expression::ObjectCtx oCtx(0);
        shared_ptr<Expression> pExpression(
	    Expression::parseObject(pBsonElement, &oCtx));
        shared_ptr<DocumentSourceFilter> pFilter(
            DocumentSourceFilter::create(pExpression));

        return pFilter;
    }

    shared_ptr<DocumentSourceFilter> DocumentSourceFilter::create(
        const shared_ptr<Expression> &pFilter) {
        shared_ptr<DocumentSourceFilter> pSource(
            new DocumentSourceFilter(pFilter));
        return pSource;
    }

    DocumentSourceFilter::DocumentSourceFilter(
        const shared_ptr<Expression> &pTheFilter):
	DocumentSourceFilterBase(),
        pFilter(pTheFilter) {
    }

    void DocumentSourceFilter::toMatcherBson(BSONObjBuilder *pBuilder) const {
	pFilter->toMatcherBson(pBuilder, 0);
    }
}
