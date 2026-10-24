import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { Readable } from 'node:stream';

export interface Listening {
  /** `http://127.0.0.1:<port>` */
  readonly url: string;
  close(): Promise<void>;
}

/** Serves a fetch handler from Node's `http` server on a free port of 127.0.0.1. */
export async function listen(handler: (request: Request) => Promise<Response>): Promise<Listening> {
  const server = createServer((req, res) => {
    respond(handler, req, res).catch((err: unknown) => {
      if (!res.headersSent) res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end(String(err));
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${port}`,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.closeAllConnections();
        server.close((err) => (err ? reject(err) : resolve()));
      }),
  };
}

async function respond(handler: (request: Request) => Promise<Response>, req: IncomingMessage, res: ServerResponse) {
  const headers = new Headers();
  for (const [name, value] of Object.entries(req.headersDistinct)) {
    for (const v of value ?? []) headers.append(name, v);
  }
  const method = req.method ?? 'GET';
  const hasBody = method !== 'GET' && method !== 'HEAD';
  const request = new Request(`http://${req.headers.host ?? '127.0.0.1'}${req.url ?? '/'}`, {
    method,
    headers,
    body: hasBody ? (Readable.toWeb(req) as ReadableStream<Uint8Array>) : undefined,
    duplex: 'half',
  } as RequestInit);
  const response = await handler(request);
  const out: Record<string, string> = {};
  response.headers.forEach((value, name) => {
    out[name] = value;
  });
  res.writeHead(response.status, out);
  if (response.body) {
    for await (const chunk of response.body) res.write(chunk);
  }
  res.end();
}
