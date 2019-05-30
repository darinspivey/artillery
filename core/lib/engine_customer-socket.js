'use strict'

const {EventEmitter} = require('events')
const async = require('async')
const debug = require('debug')
const error = debug('customer-socket:error')
const info = debug('customer-socket:info')
const verbose = debug('customer-socket:verbose')
const engineUtil = require('./engine_util')
const EngineHttp = require('./engine_http')
const io = require('socket.io-client')
const request = require('request')
const template = engineUtil.template

class CustomerSocket {
  constructor(script, ee, helpers) {
    this.script = script
    this.ee = ee
    this.helpers = helpers

    this.httpDelegate = new EngineHttp(script)
  }

  connectSocket(context, _flow_ee, cb) {
    if (context.socket) return cb()

    const target = this.script.config.target
    const socket_opts = Object.assign({}, this.script.config.socketio)
    if (!socket_opts.extraHeaders) {
      socket_opts.extraHeaders = {}
    }
    // Each user has a different auth. Inject it.
    socket_opts.extraHeaders.authorization = context.vars.authorization

    const socket = io(target, socket_opts)
    context.socket = socket

    let called_back = false

    // Make sure we only callback once, especially when an error happens
    const done = (err) => {
      if (called_back) {
        if (err) {
          // We can't call back, but we can still report the error and cleanup
          // by telling our event_name listeners to stop/error.
          _flow_ee.emit('abort', err)
        }
        return
      }
      called_back = true
      cb(err)
    }

    socket.once('connect', function() {
      verbose('Socket connected!')
      done()
    });
    socket.once('connect_error', function(err) {
      verbose('CONNECT ERROR:', err)
      done(err)
    });
    socket.once('error', function(err) {
      verbose('SOCKET ERROR:', err)
      done(err)
    })
  }

  createScenario(scenario, ee) {
    if (!scenario.authorization) {
      throw new Error('customer-socket requires an authorization value')
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
          this.connectSocket(context, _flow_ee, callback)
        }
      , (callback) => {
          // Nuke any latencies from HTTP calls
          ee.emit('reset')
          callback()
        }
      ], (err) => {
        if (err) {
          ee.emit('error', `Initialize ${err}`)
          return cb(err, context)
        }
        async.parallel([
          (callback) => {
            const event_timeout = this.script.config.socketio
              ? this.script.config.socketio.event_timeout
              : undefined

            this.listenFor({
              listen_for: scenario.listen_for
            , context
            , ee
            , _flow_ee
            , event_timeout
            }, callback)
          }
        , (callback) => {
            // Run any HTTP functions in the flow to trigger socket messages
            _flow_ee.once('listenFor:done', () => {
              this.runHttpDelegates({
                section: 'flow'
              , actions: scenario.flow
              , ee
              , context
              }, callback)
            })
          }
        ], (err) => {
          this.disconnectSocket(context)
          if (err) {
            ee.emit('error', err)
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

  disconnectSocket(context) {
    const socket = context.socket
    if (socket) {
      socket.removeAllListeners()
      socket.disconnect()
    }
  }

  listenFor(opts, cb) {
    const {
      listen_for
    , context
    , ee
    , _flow_ee
    , event_timeout
    } = opts

    if (!listen_for || !Array.isArray(listen_for)) {
      info('Warning: no "listen_for" configured. Will not receive.')
      setImmediate(() => {
        _flow_ee.emit('listenFor:done')
      })
      return cb()
    }

    const socket = context.socket

    // This is the tricky part.  We need the listeners bound first, and
    // the whole thing can't call back until they've success/failed.
    // Use an EventEmitter to signal that the listeners are ready, while
    // using the passed-in callback to return when all matches are heard.

    async.each(listen_for, (item, callback) => {
      const {
        event_name
      , match
      , capture
      , count = 1
      } = item

      if (!event_name) {
        return cb('Cannot use \'listen_for\' without an event_name')
      }

      verbose(`listen_for event name: ${event_name} (${context.vars.user_id})`)

      let startedAt = process.hrtime()
      let timer = null

      // Add other things here like `data` (exact matching)
      const validations = {
        capture: template(capture, context)
      , match: template(match, context)
      }

      let receive_count = 0

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
          let err = `Time out waiting for response match for: ${event_name}`
          ee.emit('error', err)
          _flow_ee.emit(`done:${event_name}`, err)
        }, wait_time * 1000)
      }

      const receiveEvent = (content) => {
        this.processResponse({ee, content, validations, context}, (err) => {
          receive_count++
          if (timer) {
            timer = setTimer()
          }
          let code = `${event_name} (matched)`

          if (err) {
            code = `${event_name} (FAIL)`
            ee.emit('error', code)
            error('Matching error', err)
            _flow_ee.emit(`done:${event_name}`, err)
          } else {
            verbose(`MATCH!  event_name: ${event_name} (${context.vars.user_id})`)
            if (receive_count >= count) {
              _flow_ee.emit(`done:${event_name}`)
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
      }

      // Use our EE here to ensure callback is only called once.  There could
      // be a race condition with error/success vs. timeout.
      _flow_ee.once(`done:${event_name}`, (err) => {
        socket.off(event_name, receiveEvent)
        callback(err)
      })

      timer = setTimer()

      // Listen for socket errors.  Return so the test doesn't hang.
      _flow_ee.once('abort', (err) => {
        _flow_ee.emit(`done:${event_name}`, err)
      })

      socket.on(event_name, receiveEvent)
    }, cb)

    // Signal back that the listeners have been set up so we can resume HTTP
    setImmediate(() => {
      _flow_ee.emit('listenFor:done')
    })
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
