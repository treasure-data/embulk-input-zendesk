## 0.4.4 - 2023-07-21
* [enhancement] Support cursor based incremental [#84](https://github.com/treasure-data/embulk-input-zendesk/pull/84)

## 0.4.3 - 2022-10-21
* [enhancement] Bump up to v0.4.3, built with the Gradle plugin v0.5.5 [#78](https://github.com/treasure-data/embulk-input-zendesk/pull/78)

## 0.4.2 - 2022-05-23
* [enhancement] Catchup embulk v0.10.32 [#77](https://github.com/treasure-data/embulk-input-zendesk/pull/77)

## 0.4.1 - 2022-03-29
* [enhancement] Remove deprecated functions [#76](https://github.com/treasure-data/embulk-input-zendesk/pull/76)

## 0.4.0 - 2022-03-03
* [enhancement] Release to Maven Central [#74](https://github.com/treasure-data/embulk-input-zendesk/pull/74)

## 0.3.11 - 2022-14-02
* [enhancement] catch up Embulk 0.10 [#73](https://github.com/treasure-data/embulk-input-zendesk/pull/73)

## 0.3.10 - 2020-11-10
* [enhancement] support Embulk 0.10.18 [#70](https://github.com/treasure-data/embulk-input-zendesk/pull/70)

## 0.3.9 - 2020-03-24
* [enhancement] Update UserEvent to the new API [#67](https://github.com/treasure-data/embulk-input-zendesk/pull/67)

## 0.3.8 - 2020-03-24
* [enhancement] Support `Chat` target [#65](https://github.com/treasure-data/embulk-input-zendesk/pull/65)
* [enhancement] Apply Embulk Gradle  [#64](https://github.com/treasure-data/embulk-input-zendesk/pull/64)

## 0.3.7 - 2019-08-29
* [enhancement] Replace ConfigSource#getObjectNodes method in embulk core [#63](https://github.com/treasure-data/embulk-input-zendesk/pull/63)

## 0.3.6 - 2019-07-02
* [enhancement] Improve error message [#61](https://github.com/treasure-data/embulk-input-zendesk/pull/61)
* [enhancement] Support `end_time` field for incremental [#60](https://github.com/treasure-data/embulk-input-zendesk/pull/60)

## 0.3.5 - 2019-06-03
* [enhancement] Add new targets [#58](https://github.com/treasure-data/embulk-input-zendesk/pull/58)

## 0.3.4 - 2019-04-11
* [enhancement] Add new time format [#56](https://github.com/treasure-data/embulk-input-zendesk/pull/56)

## 0.3.3 - 2019-04-11
* [fixed] Fix trailing slash [#55](https://github.com/treasure-data/embulk-input-zendesk/pull/55)

## 0.3.2 - 2019-04-10
* [fixed] Fix generate config diff based on incremental config [#54](https://github.com/treasure-data/embulk-input-zendesk/pull/54)

## 0.3.1 - 2019-04-09
* [fixed] Fix checking 404 by status code when fetching related objects [#52](https://github.com/treasure-data/embulk-input-zendesk/pull/52)

## 0.3.0 - 2019-04-08
* [enhancement] Convert embulk-input-zendesk to Java [#50](https://github.com/treasure-data/embulk-input-zendesk/pull/50)

## 0.2.14 - 2019-01-25
* [fixed] Disable pagination and add `dedup` option for non-incremental targets [#49](https://github.com/treasure-data/embulk-input-zendesk/pull/49)

## 0.2.13 - 2019-01-14
* [enhancement] Add `dedup` option, in order to avoid OOM when importing large dataset [#48](https://github.com/treasure-data/embulk-input-zendesk/pull/48)

## 0.2.12 - 2019-01-04
* [enhancement] Fix performance issue [#47](https://github.com/treasure-data/embulk-input-zendesk/pull/47)

## 0.2.11 - 2018-15-26
* [enhancement] Rate limit is fail fast instead of retry for guess/preview mode

## 0.2.10 - 2018-03-26
* [enhancement] Add Zendesk market place header

## 0.2.9 - 2017-08-03
* [fixed] `start_time` is not merged to `query`, causing infinite loop [#42](https://github.com/treasure-data/embulk-input-zendesk/pull/42)

## 0.2.8 - 2017-07-28
* [fixed] Raise `DataError` instead of `TempError` after giving up for `TempError` [#40](https://github.com/treasure-data/embulk-input-zendesk/pull/40)
* [enhancement] Only rescue `TempError` when executing thread pool, only create/shutdown pool once during retry, move retry to inside of `execute_thread_pool`

## 0.2.7 - 2017-07-19
* [fixed] Ensure thread pool is shutdown [#38](https://github.com/treasure-data/embulk-input-zendesk/pull/38)
* [enhancement] Add retry for temporary error: missing required key from JSON response

## 0.2.6 - 2017-05-23
* [enhancement] Enable incremental loading for ticket_metrics

## 0.2.5 - 2017-04-21
* [enhancement] Replace `thread` gem with `concurrent-ruby`, it's more robust and has better queue management [#33](https://github.com/treasure-data/embulk-input-zendesk/pull/33)

## 0.2.4 - 2017-04-20
* [fixed] Fix thread pool bottleneck [#31](https://github.com/treasure-data/embulk-input-zendesk/pull/31)

## 0.2.3 - 2017-04-18
* [enhancement] Add flushing to `page_builder` [#29](https://github.com/treasure-data/embulk-input-zendesk/pull/29)

## 0.2.2 - 2017-04-14
* [enhancement] Improve `httpclient` usage: re-use client instance [#27](https://github.com/treasure-data/embulk-input-zendesk/pull/27)

## 0.2.1 - 2017-04-11
* [fixed] Mem leak in `export_parallel()` method [#25](https://github.com/treasure-data/embulk-input-zendesk/pull/25)

## 0.2.0 - 2017-04-07
* [fixed] `time_metrics` is cutoff (archived), need to compare with list of all `tickets` [#23](https://github.com/treasure-data/embulk-input-zendesk/pull/23)
* [enhancement] Switch to thread pool for `export_parallel()` method [#23](https://github.com/treasure-data/embulk-input-zendesk/pull/23)

## 0.1.15 - 2017-03-30
* [fixed] Rename JRuby thread, to not expose runtime path [#21](https://github.com/treasure-data/embulk-input-zendesk/pull/21)

## 0.1.14 - 2017-03-28
* [enhancement] Concurrent fetching base target and related objects [#19](https://github.com/treasure-data/embulk-input-zendesk/pull/19)

## 0.1.13 - 2017-03-23
* [fixed] Fix to generate config_diff when no data fetched [#18](https://github.com/treasure-data/embulk-input-zendesk/pull/18)

## 0.1.12 - 2016-10-20
* [fixed] Fix `*_id` columns to be guessed as string. (e.g. external_id) [#17](https://github.com/treasure-data/embulk-input-zendesk/pull/17)

## 0.1.11 - 2016-10-20
* [fixed] Ignore `updated_at` <= `start_time` record [#16](https://github.com/treasure-data/embulk-input-zendesk/pull/16)

## 0.1.10 - 2016-10-17
* [fixed] All `*_id` columns should be type:long [#15](https://github.com/treasure-data/embulk-input-zendesk/pull/15)

## 0.1.9 - 2016-08-26
* [fixed] Ignore '422: Too recent start_time' [#14](https://github.com/treasure-data/embulk-input-zendesk/pull/14)

## 0.1.8 - 2016-07-11

* [enhancement] For huge data [#13](https://github.com/treasure-data/embulk-input-zendesk/pull/13)
* [enhancement] Improvements for non incremental export [#12](https://github.com/treasure-data/embulk-input-zendesk/pull/12)

## 0.1.7 - 2016-06-04
* [enhancement] Improvements for non incremental export [#12](https://github.com/treasure-data/embulk-input-zendesk/pull/12)

## 0.1.6 - 2016-05-09
* [fixed] Fix non-incremental export to fetch all records [#11](https://github.com/treasure-data/embulk-input-zendesk/pull/11)

## 0.1.5 - 2016-04-14
* [enhancement] Mitigate debug pain when many retry then error [#10](https://github.com/treasure-data/embulk-input-zendesk/pull/10)

## 0.1.4 - 2016-04-08

* [enhancement] Correct preview data [#9](https://github.com/treasure-data/embulk-input-zendesk/pull/9)

## 0.1.3 - 2016-03-15

* [enhancement] Support more targets [#8](https://github.com/treasure-data/embulk-input-zendesk/pull/8)
* [enhancement] Enable json type [#7](https://github.com/treasure-data/embulk-input-zendesk/pull/7)

## 0.1.2 - 2016-01-29

* [maintenance] Add authors @muga and @sakama.
* [enhancement] Add Incremental option [#6](https://github.com/treasure-data/embulk-input-zendesk/pull/6)

## 0.1.1 - 2016-01-26

* [fixed] Fix when null value given.

## 0.1.0 - 2016-01-26

The first release!!
