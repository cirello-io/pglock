# PostgreSQL Lock Client for Go

[![Build Status](https://travis-ci.org/ucirello/pglock.svg?branch=master)](https://travis-ci.org/ucirello/pglock)

[![Coverage Status](https://coveralls.io/repos/github/ucirello/pglock/badge.svg?branch=master)](https://coveralls.io/github/ucirello/pglock?branch=master)

This repository is covered by this [SLA](https://github.com/ucirello/public/blob/master/SLA.md).

The PostgreSQL Lock Client for Go is a general purpose distributed locking
library built for PostgreSQL. The PostgreSQL Lock Client for Go supports both
fine-grained and coarse-grained locking as the lock keys can be any arbitrary
string, up to a certain length. Please create issues in the GitHub repository
with questions, pull request are very much welcome.

_Recommended PostgreSQL version: 9.6 or newer_

## Logic to avoid problems with clock skew
The lock client never stores absolute times in PostgreSQL. The way locks are
expired is that a call to acquireLock reads in the current lock, checks the
RecordVersionNumber of the lock (which is a GUID) and starts a timer. If the
lock still has the same GUID after the lease duration time has passed, the
client will determine that the lock is stale and expire it.

What this means is that, even if two different machines disagree about what time
it is, they will still avoid clobbering each other's locks.
