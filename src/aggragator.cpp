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

#include "aggragator.hpp"

#include <metricq/logger/nitro.hpp>
#include <metricq/ostream.hpp>

using Log = metricq::logger::nitro::Log;

Aggregator::Aggregator(const std::string& token)
: metricq::Transformer(token), signals_(io_service, SIGINT, SIGTERM)
{
    signals_.async_wait([this](auto, auto signal) {
        if (!signal)
        {
            return;
        }
        Log::info() << "Caught signal " << signal << ". Shutdown.";
        close();
    });
}

Aggregator::~Aggregator()
{
}

void Aggregator::on_transformer_config(const metricq::json& config)
{
    for (const auto& elem : config["metrics"].items())
    {
        const auto out_metric = elem.key();
        const auto in_metric = elem.value().at("source").get<std::string>();
        const auto rate_hz = elem.value().at("rate").get<float>();
        const auto max_interval = metricq::duration_cast(std::chrono::duration<float>(1 / rate_hz));

        Log::info() << "Aggregating " << in_metric << " to " << out_metric
                    << " with a max interval of " << max_interval;
        AggregationMetric& n =
            aggregation_metrics_[in_metric].emplace_back((*this)[out_metric], max_interval);
        n.metric().metadata.rate(rate_hz);
        n.metric().metadata.scope(metricq::Metadata::Scope::last);
        n.metric().metadata["primary"] = in_metric;
        input_metrics.emplace_back(in_metric);
        // TODO check for duplicates?
    }
}

void Aggregator::on_transformer_ready()
{
    for (auto& [in_metric_name, metrics] : aggregation_metrics_)
    {
        for (auto& metric : metrics)
        {
            metric.set_metadata(metadata_[in_metric_name]);
        }
    }
}

void Aggregator::on_data(const std::string& id, const metricq::DataChunk& chunk)
{
    Transformer::on_data(id, chunk);
    for (auto& m : aggregation_metrics_.at(id))
    {
        m.flush();
    }
}

void Aggregator::on_data(const std::string& id, metricq::TimeValue tv)
{
    // TODO work on chunks, more efficient
    for (auto& m : aggregation_metrics_.at(id))
    {
        m.push(tv);
    }
}
