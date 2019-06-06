'use strict'

const {EventEmitter} = require('events')
const ws = require('ws')
const {SubscriptionClient} = require('subscriptions-transport-ws')
const request = require('request')
const async = require('async')
const debug = require('debug')
const error = debug('graphql-subscriptions:error')
const info = debug('graphql-subscriptions:info')
const verbose = debug('graphql-subscriptions:verbose')
const engineUtil = require('./engine_util')
const EngineHttp = require('./engine_http')
const template = engineUtil.template

class CustomerSocket {
  constructor(script, ee, helpers) {
    this.script = script
    this.ee = ee
    this.helpers = helpers

    this.httpDelegate = new EngineHttp(script)
  }

  connectSubscriptionClient(context, cb) {
    if (context.subscription_client) return cb()

    const target = this.script.config.target
    // Each user has a different auth. Inject it.
    const authorization = context.vars.authorization

    const subscription_client = new SubscriptionClient(target, {
      connectionParams: { authorization }
    , connectionCallback: (err) => {
        if (err) return cb(err)
        context.subscription_client = subscription_client
        verbose('Subscription client connected!')
        cb()
      }
    }, ws)

  }

  createScenario(scenario, ee) {
    if (!scenario.authorization) {
      throw new Error('graphql-subscriptions requires an authorization value')
    }

    const executeScenario = (context, cb) => {
      ee.emit('started')

      context._successCount = 0
      context._jar = request.jar()
      context._pendingRequests = 0
      const _flow_ee = new EventEmitter()

      // 1. Run The 'initialize' section sequentially. Load context vars, etc.
      // 2. Make sure we have an `authorization`
      // 3. Create/connect the socket
      // 4. Bind listeners.  These will WAIT for socket data and thus cannot
      //    callback immediately.  Do them asynchronously and use an EE for
      //    signaling back to the main loop.
      // 5. Run 'flow', which can contain HTTP calls that generate WS messages

      async.series([
        (callback) => {
          this.runHttpDelegates({
            section: 'initialize'
          , actions: scenario.initialize
          , ee
          , context
          }, callback)
        }
      , (callback) => {
          const auth = this.helpers.template(scenario.authorization, context)
          context.vars.authorization = auth
          callback()
        }
      , (callback) => {
          this.connectSubscriptionClient(context, callback)
        }
      , (callback) => {
          // Nuke any latencies from HTTP calls
          ee.emit('reset')
          callback()
        }
      ], (err) => {
        if (err) {
          const msg = typeof err === 'object' && err.message
            ? err.message
            : err
          ee.emit('error', `Initialize ${msg}`)
          return cb(err, context)
        }
        async.parallel([
          (callback) => {
            const event_timeout = this.script.config.subscriptions
              ? this.script.config.subscriptions.event_timeout
              : undefined

            this.makeSubscriptions({
              subscriptions: scenario.subscriptions
            , context
            , ee
            , _flow_ee
            , event_timeout
            }, callback)
          }
        , (callback) => {
            // Run any HTTP functions in the flow to trigger socket messages
            _flow_ee.once('makeSubscriptions:done', () => {
              this.runHttpDelegates({
                section: 'flow'
              , actions: scenario.flow
              , ee
              , context
              }, callback)
            })
          }
        ], (err) => {
          this.disconnectSubscriptionClient(context)
          if (err) {
            const msg = typeof err === 'object' && err.message
              ? err.message
              : err
            ee.emit('error', msg)
            error('Error in flow', err)
            return cb(err, context)
          }
          ee.emit('done')
          cb(null, context)
        })
      })
    }

    return executeScenario
  }

  disconnectSubscriptionClient(context) {
    const subscription_client = context.subscription_client
    if (subscription_client) {
      subscription_client.unsubscribeAll()
      subscription_client.close()
    }
  }

  makeSubscriptions(opts, cb) {
    const {
      subscriptions
    , context
    , ee
    , _flow_ee
    , event_timeout
    } = opts

    if (!subscriptions || !Array.isArray(subscriptions)) {
      info('Warning: no "subscriptions" configured. Will not receive.')
      setImmediate(() => {
        _flow_ee.emit('makeSubscriptions:done')
      })
      return cb()
    }

    const subscription_client = context.subscription_client

    // This is the tricky part.  We need the subscriptions made first, and
    // the whole thing can't call back until they've success/failed.
    // Use an EventEmitter to signal that they are ready, while
    // using the passed-in callback to return when all matches are heard.

    async.each(subscriptions, (item, callback) => {
      const {
        query_name
      , variables
      , match
      , capture
      , count = 1
      } = item
      const member_id = context.vars.member_id

      if (!query_name) {
        return cb('Each subscription requires a `query_name`')
      }

      const query = this.getQueryByName(query_name, callback)

      if (!query) {
        return cb(`Query name "${query_name}" not found`)
      }

      const sub_name = query.match(/subscription\s*(.+?)\s*\{/)[1]
      verbose(`Adding subscription: ${sub_name} (${member_id})`)

      let startedAt = process.hrtime()

      // Add other things here like `data` (exact matching)
      const validations = {
        capture: template(capture, context)
      , match: template(match, context)
      }

      let receive_count = 0
      let timer = null

      // Timeout can be disabled by setting timer to -1.  Allows for
      // continuous messages (via message count) to be received
      const setTimer = () => {
        // If we don't get a response within the timeout, fire an error
        if (timer) {
          clearTimeout(timer)
        }
        const wait_time = !event_timeout
          ? 10
          : event_timeout

        if (wait_time < 0) {
          verbose('warning: event_timeout has been disabled!')
          return
        } else {
          verbose(`Event timeout is: ${event_timeout}`)
        }
        if (receive_count >= count) {
          verbose(`Complete. ${receive_count} of ${count} events received.`)
          return
        }
        return setTimeout(function responseTimeout() {
          let err = `Time out waiting for response match for: ${sub_name}`
          _flow_ee.emit(`done:${sub_name}`, err)
        }, wait_time * 1000)
      }

      timer = setTimer()

      const subscription = subscription_client.request({
        query
      , variables
      }).subscribe({
        next: (content) => {
          if (content.errors) {
            for (const err of content.errors) {
              ee.emit('error', err)
              error(`${sub_name} error`, err)
            }
            _flow_ee.emit(`done:${sub_name}`, `${sub_name} error`)
            return
          }

          this.processResponse({ee, content, validations, context}, (err) => {
            receive_count++
            if (timer) {
              timer = setTimer()
            }
            let code = `${sub_name} (matched)`

            if (err) {
              code = `${sub_name} (FAIL)`
              ee.emit('error', code)
              error('Matching error', err)
              _flow_ee.emit(`done:${sub_name}`, err)
            } else {
              verbose(`Received ${sub_name} payload! (${member_id})`)
              if (receive_count >= count) {
                _flow_ee.emit(`done:${sub_name}`)
              }
            }
            // If we've captured a field to use for latency calculation, use it
            // instead of the start time of this listener.
            if (context.vars.__calculate_latency) {
              const pub_date = new Date(context.vars.__calculate_latency)
              if (pub_date instanceof Date && !isNaN(pub_date)) {
                const now = new Date()
                // There will be clock drift here, but at least it tells us
                // when things get SLOW, on the order of seconds
                let delta = (now - pub_date) * 1e6
                if (delta < 0) {
                  // Don't let negatives (clock drift) skew the latency report
                  delta = 0
                }
                ee.emit('response', delta, code, context._uid)
                return
              }
            }
            this.markEndTime(ee, context, code, startedAt)
          })

          _flow_ee.emit(`done:${sub_name}`)
        }
      , error(err) {
          _flow_ee.emit(`done:${sub_name}`, err)
        }
      })

      // Use our EE here to ensure callback is only called once.  There could
      // be a race condition with error/success vs. timeout.
      _flow_ee.once(`done:${sub_name}`, (err) => {
        if (subscription && typeof subscription.unsubscribe === 'function') {
          subscription.unsubscribe()
        }
        callback(err)
      })

      timer = setTimer()

    }, cb)

    // Signal back that the listeners have been set up so we can resume HTTP
    setImmediate(() => {
      _flow_ee.emit('makeSubscriptions:done')
    })
  }

  getQueryByName(query_name) {
    return this.script.config.processor.getSubscriptionQuery(query_name)
  }

  markEndTime(ee, context, code, startedAt) {
    const endedAt = process.hrtime(startedAt)
    const delta = (endedAt[0] * 1e9) + endedAt[1]
    ee.emit('response', delta, code, context._uid)
  }

  processResponse(opts, cb) {
    const {
      ee
    , content
    , validations
    , context
    } = opts

    // If no capture or match specified, then we consider it a success at this point...
    if (!validations || !Object.keys(validations).length) {
      return cb()
    }

    // Fake an HTTP response since the comparison is done by the http engine.
    const fauxResponse = {body: JSON.stringify(content)}

    // Handle the capture or match clauses...
    engineUtil.captureOrMatch(
      validations
    , fauxResponse
    , context
    , (err, result) => {
      // Were we unable to invoke captureOrMatch?
      if (err) {
        return cb(err)
      }

      // Do we have any failed matches?
      const failed_matches = []
      const success_matches = []
      for (const v of Object.values(result.matches)) {
        if (!v.success) {
          failed_matches.push(v)
        } else {
          success_matches.push(v)
        }
      }

      // How to handle failed matches?
      if (failed_matches.length > 0) {
        // TODO: Should log the details of the match somewhere
        ee.emit('error', 'Failed match XYZ')
        return cb(new Error('Failed response match'))
      }
      // Emit match events...
      // TODO - do we need to do this, Darin?!
      for (const v of success_matches) {
        ee.emit('match', v.success, {
          expected: v.expected,
          got: v.got,
          expression: v.expression
        })
      }

      // Populate the context with captured values
      for (const [k, v] of Object.entries(result.captures)) {
        context.vars[k] = v
      }

      // TODO - WTF is this, Darin?!!!
      // Replace the base object context
      // Question: Should this be JSON object or String?
      context.vars.$ = fauxResponse.body;

      // Increment the success count...
      context._successCount++;

      return cb()
    });
  }

  runHttpDelegates(opts, cb) {
    const {
      section = 'http'
    , actions
    , ee
    , context
    } = opts

    // Receives a list of HTTP actions (from initialize or flow)
    if (!actions || !actions.length) return cb()
    if (!Array.isArray(actions)) {
      error(`Section of ${section} actions expects an array.`, actions)
      throw new TypeError(`${section} actions are invalid`)
    }
    const tasks = actions.map((task) => {
      return this.httpDelegate.step(task, ee);
    })
    context._pendingRequests += tasks.length

    async.eachSeries(tasks, (httpFn, callback) => {
      httpFn(context, (err) => {
        context._pendingRequests--
        setImmediate(callback, err)
      })
    }, cb)
  }


}

module.exports = CustomerSocket
