import Fastify, { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import fastifyAmqpAsync from 'fastify-amqp-async'
import { config } from 'dotenv'

config()

const fastify = Fastify()

const PORT = process.env.PORT || '3000'
const AMQP_URL = process.env.AMQP_URL || 'amqp://localhost/'
const JOB_EXCHANGE = process.env.JOB_EXCHANGE || 'job'
const JOB_ROUTING_KEY = process.env.JOB_ROUTING_KEY || ''
const API_KEYS = JSON.parse(process.env.API_KEYS || '[]')

fastify.register(fastifyAmqpAsync, {
  connectionString: AMQP_URL,
  useRegularChannel: true,
})

const authenticateRequest = async (
  request: FastifyRequest,
  reply: FastifyReply,
) => {
  const apiKey = request.headers['x-api-key']
  if (!apiKey || !API_KEYS.includes(apiKey)) {
    reply.code(401).send({ error: 'Unauthorized' })
  }
}

const setupJobRoute = async (fastify: FastifyInstance) => {
  fastify.post(
    '/',
    { preHandler: authenticateRequest },
    async (request: FastifyRequest, reply: FastifyReply) => {
      try {
        const channel = fastify.amqp.channel
        await channel.assertExchange(JOB_EXCHANGE, 'direct')
        await channel.publish(
          JOB_EXCHANGE,
          JOB_ROUTING_KEY,
          Buffer.from(JSON.stringify(request.body)),
        )

        reply.send({ status: 'Job Submitted' })
      } catch (error) {
        reply.code(500).send({ error: 'Failed to submit job' })
      }
    },
  )
}

const setupRootRoute = async (fastify: FastifyInstance) => {
  fastify.get('/', async (_request: FastifyRequest, reply: FastifyReply) => {
    reply.send({ status: 'Server is running' })
  })
}

fastify.register(setupJobRoute, { prefix: '/jobs' })
fastify.register(setupRootRoute, { prefix: '/' })

fastify.listen({ port: +PORT, host: '0.0.0.0' }, (err: any) => {
  if (err) throw err
  console.log(`Server running on port ${PORT}`)
})
