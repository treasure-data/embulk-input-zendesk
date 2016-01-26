[![Build Status](https://travis-ci.org/treasure-data/embulk-input-zendesk.svg?branch=master)](https://travis-ci.org/treasure-data/embulk-input-zendesk)
[![Code Climate](https://codeclimate.com/github/treasure-data/embulk-input-zendesk/badges/gpa.svg)](https://codeclimate.com/github/treasure-data/embulk-input-zendesk)
[![Test Coverage](https://codeclimate.com/github/treasure-data/embulk-input-zendesk/badges/coverage.svg)](https://codeclimate.com/github/treasure-data/embulk-input-zendesk/coverage)
[![Gem Version](https://badge.fury.io/rb/embulk-input-zendesk.svg)](https://badge.fury.io/rb/embulk-input-zendesk)

# Zendesk input plugin for Embulk

Embulk input plugin for loading [Zendesk](https://www.zendesk.com/) records.

## Overview

Required Embulk version >= 0.8.1.

**NOTE** This plugin don't support JSON type columns e.g. custom fields, tags, etc for now. But they will be supported soon.

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: yes

## Configuration

- **login_url**: Login URL for Zendesk (string, required)
- **auth_method**: `basic`, `token`, or `oauth`. For more detail on [zendesk document](https://developer.zendesk.com/rest_api/docs/core/introduction#security-and-authentication). (string, required)
- **target**: Which export Zendesk resource. Currently supported are `tickets`, `ticket_events`, `users`, `organizations`, `ticket_fields` or `ticket_forms`. (string, required)
- **username**: The user name a.k.a. email. Required if `auth_method` is `basic` or `token`. (string, default: `null`)
- **password**: Password. required if `auth_method` is `basic`. (string, default: `null`)
- **token**: Token. required if `auth_method` is `token`. (string, default: `null`)
- **access_token**: OAuth Access Token. required if `auth_method` is `oauth`. (string, default: `null`)
- **start_time**: Start export from this time if present. (string, default: `null`)
- **retry_limit**: Try to retry this times (integer, default: 5)
- **retry_initial_wait_sec**: Wait seconds for exponential backoff initial value (integer, default: 1)

## Example

```yaml
in:
  type: zendesk
  login_url: https://obscura.zendesk.com
  auth_method: token
  username: jdoe@example.com
  token: 6wiIBWbGkBMo1mRDMuVwkw1EPsNkeUj95PIz2akv
  target: tickets
  start_time: "2015-01-01 00:00:00+0000"
```


## Test

```
$ rake test
```
