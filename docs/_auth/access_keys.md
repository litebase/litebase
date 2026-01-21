# Access Keys

Access Keys are a secure way to provide programmatic access to your Litebase resources. They are designed for use in applications, scripts, and other automated workflows where a user is not present.

An access key is composed of two parts:

- **Access Key ID**: A public, non-secret identifier for the key (e.g., `lbakid_...`).
- **Access Key Secret**: A private, secret value that is used to sign API requests.

The secret is provided only once upon creation of the access key and cannot be retrieved later. If the secret is lost, the key must be rotated to generate a new secret, or a new key must be created.

## Request Signing

To authenticate an API request with an access key, the request must be signed. Litebase uses a custom HMAC-based signature algorithm that is similar to other cloud providers' signing processes.

The signature is created by building a canonical request string that includes the HTTP method, the request path, a specific set of headers (`Content-Type`, `Host`, `X-Litebase-Date`), query parameters, and a hash of the request body. This string is then used to create a series of HMAC-SHA256 hashes, with the final signature being derived from the Access Key Secret.

The final signature is sent in the `Authorization` header, along with the Access Key ID and a list of the headers that were included in the signature calculation.

The server performs the same signature calculation on its side. If the calculated signature matches the one provided in the request, the request is considered authentic. This process ensures that the request has not been tampered with in transit and that the sender is in possession of the correct Access Key Secret.

## Lifecycle Management

Access keys can be managed through the Litebase API or CLI. The typical lifecycle of an access key is as follows:

1. **Creation**: An access key is created with a set of permissions (statements). The Access Key ID and Secret are returned.
2. **Usage**: The key is used by an application to sign API requests.
3. **Rotation**: To rotate credentials, create a new access key with the required permissions, update your applications to use the new Access Key ID and Secret, and then delete the old key to revoke access.
4. **Deletion**: When the key is no longer needed, it can be deleted. This action is irreversible.

## Permissions

Each access key has a set of `Statement`s attached to it, which define what actions it is authorized to perform. When a request is authenticated, these statements are evaluated to determine if the requested action on the specified resource is allowed. It is a security best practice to grant only the minimum necessary permissions to an access key.
