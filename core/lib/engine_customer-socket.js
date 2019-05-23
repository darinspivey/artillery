'use strict'

const {EventEmitter} = require('events')
const async = require('async');
const debug = require('debug')('customer-socket');
const engineUtil = require('./engine_util');
const EngineHttp = require('./engine_http');
const io = require('socket.io-client');
const request = require('request');
const template = engineUtil.template;

class CustomerSocket {
  constructor(script, ee, helpers) {
    this.script = script
    this.ee = ee
    this.helpers = helpers

    this.httpDelegate = new EngineHttp(script)
  }

  connectSocket(context, cb) {
    if(!context.socket) {
      const target = this.script.config.target
      const socket_opts = Object.assign({}, this.script.config.socketio)
      if (!socket_opts.extraHeaders) {
        socket_opts.extraHeaders = {}
      }
      // Each user has a different auth. Inject it.
      socket_opts.extraHeaders.authorization = context.vars.authorization

      const socket = io(target, socket_opts)
      context.socket = socket

      socket.once('connect', function() {
        cb()
      });
      socket.once('connect_error', function(err) {
        cb(err)
      });
      socket.once('error', function(err) {
        cb(err)
      })
    }
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
          this.connectSocket(context, callback)
        }
      ], (err) => {
        if (err) return cb(err)

        async.parallel([
          (callback) => {
            this.listenFor({
              listen_for: scenario.listen_for
            , context
            , ee
            , _flow_ee
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
          ee.emit('done')
          if (err) {
            debug('Error in initialize', err)
            return cb(err)
          }
          cb()
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
    } = opts

    if (!listen_for || !Array.isArray(listen_for)) {
      debug('Warning: no "listen_for" configured. Will not receive.')
      setImmediate(() => {
        _flow_ee.emit('listenFor:done')
      })
      return cb()
    }

    // This is the tricky part.  We need the listeners bound first, and
    // the whole thing can't call back until they've success/failed.
    // Use an EventEmitter to signal that the listeners are ready, while
    // using the passed-in callback to return when all matches are heard.

    async.each(listen_for, (item, callback) => {
      const {
        event_name
      , match
      } = item

      if (!event_name) {
        return cb('Cannot use \'listen_for\' without an event_name')
      }

      debug(`listen_for event name: ${event_name} (${context.vars.user_id})`)

      const startedAt = process.hrtime()

      // Add things here like `capture` or `data` (exact matching)
      const validations = {
        match: template(match, context)
      }

      const socket = context.socket

      const receiveEvent = (content) => {
        this.processResponse({ee, content, validations, context}, (err) => {
          clearTimeout(timer)
          let code = `${event_name} (matched)`

          if (err) {
            code = `${event_name} (FAIL)`
            debug('Matching error', err)
            callback(err)
          } else {
            debug(`MATCH!  event_name: ${event_name} (${context.vars.user_id})`)
            callback()
          }
          this.markEndTime(ee, context, code, startedAt)
        })
      }

      // If we don't get a response within the timeout, fire an error
      let wait_time = this.script.config.timeout || 10

      const timer = setTimeout(function responseTimeout() {
        socket.off(event_name, receiveEvent)
        let err = `Time out waiting for response match for: ${event_name}`
        ee.emit('error', err)
        return callback(err)
      }, wait_time * 1000)

      socket.once(event_name, receiveEvent)
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
    if (!validations.match) {
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
      debug(`Section of ${section} actions expects an array.`, actions)
      throw new TypeError(`${section} actions are invalid`)
    }
    const tasks = actions.map((task) => {
      return this.httpDelegate.step(task, ee);
    })
    context._pendingRequests += tasks.length

    async.each(tasks, (httpFn, callback) => {
      httpFn(context, (err) => {
        context._pendingRequests--
        callback(err)
      })
    }, cb)
  }


}

module.exports = CustomerSocket
