# Mandatory-Activity-5
== A Distributed Auction System ==

::Introduction::

You must implement a **distributed auction system** using replication:
a distributed component which handles auctions, and provides
operations for bidding and querying the state of an auction. The
component must faithfully implement the semantics of the system
described below, and must at least be resilient to one (1) crash
failure.

::MA Learning Goal::

The goal of this mandatory activity is that you learn (by doing) how
to use replication to design a service that is resilent to crashes. In
particular, it is important that you can recognise what the key issues
that may arise are and understand how to deal with them.

 

::API::

Your system must be implemented as some number of nodes, running on
distinct processes (no threads). Clients direct API requests to any
node they happen to know (it is up to you to decide how many nodes can
be known). Nodes must respond to the following API

Method:  bid
Inputs:  amount (an int)
Outputs: ack
Comment: given a bid, returns an outcome among {fail, success or exception}

 

Method:  result
Inputs:  void
Outputs: outcome
Comment:  if the auction is over, it returns the result, else highest bid.

 

::Semantics::

Your component must have the following behaviour, for any reasonable
sequentialisation/interleaving of requests to it:

- The first call to "bid" registers the bidder.

- Bidders can bid several times, but a bid must be higher than the
  previous one(s).

- after a predefined timeframe, the highest bidder ends up as the
  winner of the auction, e.g, after 100 time units from the start of
  the system.

- bidders can query the system in order to know the state of the
  auction.

 

:: Faults :: 

- Assume a network that has reliable, ordered message transport, where
  transmissions to non-failed nodes complete within a known
  time-limit.

- Your component must be resilient to the failure-stop failure of one
  (1) node.

 

:: Report ::

Write a 2-page report (if necessary you can go up to 3 pages)
containing the following structure (exactly create four sections as
below):

- Introduction. A short introduction to what you have done.

- Architecture. A description of the architecture of the system and
  its protocols (behaviour), including any protocols used internally
  between nodes of the system.

- Correctness 1. Argue whether your implementation satisfies
  linearisability or sequential consistency. In order to do so, first,
  you must state precisely what such property is.

- Correctness 2. An argument that your protocol is correct in the
  absence and the presence of failures.
