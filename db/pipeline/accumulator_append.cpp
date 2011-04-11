/**
 * Copyright (c) 2011 10gen Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "pch.h"
#include "accumulator.h"

#include "db/pipeline/value.h"

namespace mongo {
    boost::shared_ptr<const Value> AccumulatorAppend::evaluate(
        boost::shared_ptr<Document> pDocument) const {
        assert(vpOperand.size() == 1);
	boost::shared_ptr<const Value> prhs(vpOperand[0]->evaluate(pDocument));
        vpValue.push_back(prhs);

        return Value::getNull();
    }

    boost::shared_ptr<const Value> AccumulatorAppend::getValue() const {
        return Value::createArray(vpValue);
    }

    AccumulatorAppend::AccumulatorAppend():
        Accumulator(),
        vpValue() {
    }

    boost::shared_ptr<Accumulator> AccumulatorAppend::create() {
	boost::shared_ptr<AccumulatorAppend> pAccumulator(
	    new AccumulatorAppend());
        return pAccumulator;
    }

    const char *AccumulatorAppend::getName() const {
	return "$append";
    }
}
