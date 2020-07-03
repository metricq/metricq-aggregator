// Copyright (c) 2018, ZIH, Technische Universitaet Dresden, Federal Republic of Germany
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright notice,
//       this list of conditions and the following disclaimer in the documentation
//       and/or other materials provided with the distribution.
//     * Neither the name of metricq nor the names of its contributors
//       may be used to endorse or promote products derived from this software
//       without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include "aggregation_metric.hpp"

AggregationMetric::AggregationMetric(metricq::Transformer::Metric& write_metric,
                                     metricq::Duration max_interval)
: write_metric_(write_metric), max_interval_(max_interval)
{
    // we flush manually for each chunk we receive
    write_metric_.chunk_size(0);
}

void AggregationMetric::push(metricq::TimeValue tv)
{
    if (count_ == 0)
    {
        first_sample_time_ = tv.time;
        last_sample_time_ = tv.time;
        sum_ = tv.value;
        count_ = 1;
        return;
    }
    if (tv.time - first_sample_time_ > max_interval_)
    {
        // LAST semantic as usual for power metrics
        // Currently only unweighted averaging, no integration
        write_metric_.send({ last_sample_time_, sum_ / count_ });

        first_sample_time_ = tv.time;
        last_sample_time_ = tv.time;
        sum_ = tv.value;
        count_ = 1;
        return;
    }
    last_sample_time_ = tv.time;
    sum_ += tv.value;
    count_++;
}

void AggregationMetric::flush()
{
    write_metric_.flush();
}

void AggregationMetric::set_metadata(const metricq::Metadata& metadata)
{
    for (auto metadatum = metadata.begin(); metadatum != metadata.end(); metadatum++)
    {
        if (metadatum.key() == "rate" || metadatum.key() == "scope" ||
            metadatum.key() == "historic" || metadatum.key() == "date")
        {
            continue;
        }

        write_metric_.metadata[metadatum.key()] = metadatum.value();
    }
}
