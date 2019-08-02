# oDL

Open Downloads (oDL) is built on the simple idea that podcast download numbers should be consistent and transparent across hosting companies, networks, and analytics providers.

oDL is an open source package that contains a simple spec, blacklists, and code to prepare log files and count podcast downloads in a scalable way.

oDL's goal is to move the podcast industry forward collectively by introducing a layer of trust between podcasters, providers, and advertisers by removing the black box approach and replacing it with an open system to count and verify download numbers.

## Quickstart

oDL is in the comment phase of development. For now you'll need to clone from source.

```
$ git clone git@github.com:open-downloads/odl.git && cd odl
$ virtualenv .
$ source bin/activate
$ ipython

> from odl import prepare, pipeline
> prepare.run('path/to/events-input.csv', 'path/to/events.odl.avro')
> pipeline.run('path/to/events.odl.avro', 'path/to/events-output')
Running the oDL pipeline from path/to/events.odl.avro to path/to/events-output

oDL run complete.

Downloads: 13751

path/to/events-output/count.txt
path/to/events-output/hourly.csv
path/to/events-output/episodes.csv
path/to/events-output/apps.csv
```

## Overview

oDL is a fundamentally new paradigm in podcasting, so we wanted to explain a little more about where oDL fits in the ecosystem.

### Spec

An oDL Download meets the following criteria:

- It's an HTTP GET request
- The IP Address is not on the blacklist
- The User Agent is not on the blacklist
- It can't be a streaming request for the first 2 bytes (i.e. Range: bytes=0-1)
- The User Agent/IP combination counts once per day (i.e. fixed 24 hour period starting at midnight UTC)

That's it. It's intentionally simple to reduce confusion for all involved.

### Dealing with bots

Podcasts get many downloads from bots, servers, and things that just aren't human. We need to avoid counting these, but if everyone maintains their own blacklist, they can't count the same way.

oDL uses common, publicly available blacklists for User Agent and IP Address:

- OPAWG's Podcast User Agent list: https://github.com/opawg/user-agents
- ipcat's datacenter IP list: https://github.com/client9/ipcat/blob/master/datacenters.csv

### Out of Scope.

oDL focuses on accuracy in cross provider download numbers. Analytics Prefixes and hosting providers have access to different sets of data, so we take the intersection of this information. All counts are based on these seven data points:

- IP Address
- User Agent
- HTTP Method
- Timestamp
- Episode Identifier (ID/Enclosure URL)
- Byte Range Start
- Byte Range End

oDL does not take into account bytes coming off the server or even percentage of episode streamed. These numbers tend to conflate listening behavior with downloads and only podcast players can reliably report listening behavior.

Our goal is to simply weed out requests that did not make a good faith effort to put an audio file on a podcast player.

### Isn't this just IAB v2.0?

No, similar goals, but different tactics.

The IAB v2.0 spec is great, but it relies on wording around "Best Practices." We believe that a spec shouldn't have best practices. Two hosting providers, both who are IAB v2.0 certified could have up to a 10% difference in download counts. This creates confusion for publishers, podcasters and advertisers alike as who's number is "correct".

IAB v2.0 is also expensive. It's up to $45k for a hosting provider to become certified. Competition is important, and this hurdle creates an undue burden on smaller companies.

oDL takes a transparent, open source approach. By using common blacklists of IPs and User Agents we make the industry as a whole more accurate.

oDL and IAB v2.0 are not mutually exclusive. A provider may decide to do both.

### Advanced Analytics

Many hosting providers offer advanced analytics and we are all about it. oDL is not meant to reduce innovation in the podcast analytics space. Simplecast is [fingerprinting downloads](https://blog.simplecast.com/unique-listeners-a-brand-new-metric-for-the-podcasting-industry-from-simplecast/), [backtracks](https://backtracks.fm/podcast-analytics) has its thing going on and many others have interesting ideas of how to use download data.

Advanced analytics methodologies are not consistent across hosting providers. Some providers will use a whitelist to allow more downloads from IPs with many users or use shorter attribution windows. These methods, while taken in good faith, inflate download numbers against providers that take a stricter approach.

Ad-supported podcasters can make more or less money depending on which hosting provider they choose.

Our hope is that an oDL number sits beside the advanced analytics number and may be higher or lower, but the podcaster knows that it is consistent with their peers.

### I'm a Hosting Provider and I want to support oDL.

oDL is a self-certifying spec, meaning that there is no formal process to become certified. The only requirement is that you let podcasters download their raw logs in the odl.avro format.

Users can then verify the numbers reported in your dashboard are the same as oDL's. Our goal in the future is to add a hosted verification service to `https://odl.dev`.

## Code

A spec isn't much without code to run it all. oDl ships with a full implementation for counting downloads against server logs. It's built using Apache Beam to scale from counting thousands of downloads on your laptop to millions using a data processing engine like Apache Spark or Google Cloud Dataflow.

### Prepare

The first step in running oDL is to prepare the data for the download counting job. oDL uses the following avro schema for raw events:

```
[{
    'name': 'encoded_ip',
    'type': 'string'
}, {
    'name': 'user_agent',
    'type': 'string'
}, {
    'name': 'http_method',
    'type': 'string'
}, {
    'name': 'timestamp',
    'type': 'string'
}, {
    'name': 'episode_id',
    'type': 'string'
}, {
    'name': 'byte_range_start',
    'type': ["int", "null"]
}, {
    'name': 'byte_range_end',
    'type': ["int", "null"]
}]
```

We ask that you encode the IP before creating the odl.avro file. We provide a helper to salt and hash the IP.

```
> from odl.prepare import get_ip_encoder
> encode = get_ip_encoder()
> encode('1.2.3.4')
'58ecdbd64d2fa9844d29557a35955a58'
> encode('1.2.3.5')
'1bcdcb404b16e046f3a13fc5563853d3'
> encode('1.2.3.5')
 '1bcdcb404b16e046f3a13fc5563853d3'
```

By default `get_ip_encoder` uses a random salt, so if you need to run oDL over multiple files, include your own salt.

```
> encode = get_ip_encoder(salt="this is a super secret key")
> encode('1.2.3.4')
```

To actually write a file, you can use the following.

```
> from odl import prepare
> prepare.run('./path/to/log-events.json', './path/to/events.odl.avro', format="json")
```

Write will throw errors at you if it can't create a file.

### Pipeline

The `odl` package uses Apache Beam under the hood to work on logfiles at any scale. On your local machine it looks like this:

> pipeline.run('path/to/events.odl.avro', 'path/to/events-output')

```
> from odl import pipeline
> pipeline.run('path/to/events.odl.avro', 'path/to/events-output')
```

If you would like to run this through Google Cloud DataFlow on a large scale dataset, you would use the following:

*Note: This costs money, since it's using Google Cloud Platform.*

```
> pipeline.run('gs://events-bucket-name/events*',  
  'gs://output-bucket-namep/events-output',
  {"runner" : "DataflowRunner", 'project': 'gc-org-name'})
```

## Closing

It's still early in the ball game here, but we hope open, transparent counting will improve podcasting for all.
