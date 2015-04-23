(function() {
    var Topic = React.createClass({
        render: function() {
            var tweetTopic = this.props.info["topic"],
                topicCount = this.props.info["count"];

            return (
                <tr className={this.props.info["highlight"]}>
                    <td>{tweetTopic}</td>
                    <td className={"text-center"}>{topicCount}</td>
                </tr>
            );
        }
    });

    var TopicList = React.createClass({
        getInitialState: function() {
            return {data: []};
        },

        componentDidMount: function() {
            this.sse0 = new EventSource("/statistic");

            this.sse0.onmessage = function(msg) {
                var tweetStatistic = JSON.parse(msg.data);

                this.props.tweetChart.update(tweetStatistic["total"]);
            }.bind(this);

            this.sse1 = new EventSource("/tweet");

            this.sse1.onmessage = function(msg) {
                console.log(msg.data);

                var tweetInfos = JSON.parse(msg.data).sort(
                        function(t1, t2) {
                            return t2["count"] - t1["count"];
                        }
                    );

                var data = this.mergeData(this.state.data, tweetInfos);
                this.setState({data: data});
            }.bind(this);
        },

        mergeData: function(old, young) {
            var tweets = young.map(function(tweet) {
                if (old.length === 0) {
                    tweet["highlight"] = "";
                    return tweet;
                }

                tweet["highlight"] = "";

                //tweet["highlight"] = "warning";
                //for(var i = 0; i < old.length; i++) {
                //    if(old[i]["topic"] === tweet["topic"]) {
                //        if (tweet["count"] < old[i]["count"]) {
                //            tweet["highlight"] = "danger";
                //        } else if (tweet["count"] > old[i]["count"]) {
                //            tweet["highlight"] = "success";
                //        } else {
                //            tweet["highlight"] = "";
                //        }
                //
                //        break;
                //    }
                //}

                return tweet;
            });

            return tweets;
        },

        render: function() {
            var tweetInfos = this.state.data.map(function(tweetInfo) {
                return (<Topic info={tweetInfo} />);
            });

            return (
                <div className={"panel panel-primary"}>
                    <div className={"panel-heading"}>
                        <span className={"glyphicon glyphicon-tasks"}></span>

                        <strong>Popular Topics</strong>
                    </div>

                    <table className={"table"}>
                        <thead>
                            <th>Topic</th>
                            <th className={"text-center"}>Count</th>
                        </thead>

                        <tbody>{tweetInfos}</tbody>
                    </table>
                </div>
            );
        }
    });

    /* Application */
    var App = function() {
        if (!(this instanceof App)) {
            return new App();
        }

        return this;
    };

    App.prototype.start = function(container, tweetChart) {
        React.render(<TopicList tweetChart={tweetChart}/>, container);
    };

    var TweetChart = function() {
        if (!(this instanceof TweetChart)) {
            return new TweetChart();
        }

        return this;
    };

    TweetChart.prototype.render = function(id) {
        var self = this;

        $('#'+id).highcharts('StockChart', {
            chart: {
                events: {
                    load: function () {
                        self.series = this.series[0];
                    }
                }
            },

            rangeSelector: {
                buttons: [{
                    count: 2,
                    type: 'minute',
                    text: '2M'
                }, {
                    count: 5,
                    type: 'minute',
                    text: '5M'
                }, {
                    type: 'all',
                    text: 'All'
                }],
                selected: 0,
                inputEnabled: false
            },

            title : {
                text : 'Tweet Frequency'
            },

            exporting: {
                enabled: false
            },

            series: [{
                name: 'tweets/sec',
                data: (function () {
                    var data = [], time = (new Date()).getTime();

                    for (var i = -999; i <= 0; i += 1) {
                        data.push([time + i * 1000, 0]);
                    }

                    return data;
                }())
            }]
        });
    };

    TweetChart.prototype.update = function(data) {
        var x = (new Date()).getTime();

        this.series.addPoint([x, data], true, true);
    };

    Highcharts.setOptions({
        global : {
            useUTC : false
        }
    });

    var app = new App();
    var tweetChart = new TweetChart();

    tweetChart.render('chart');
    app.start(document.getElementById("app"), tweetChart);

    var map = new Datamap({
        element: document.getElementById('map'),
        scope: 'usa'
    });
})();