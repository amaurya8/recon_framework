import base64


class CryptoUtil:
    """Utility class for encrypting and decrypting passwords without requiring a key"""

    SECRET = "CustomSecretPhrase"  # Define a secret logic for shifting characters

    @staticmethod
    def _get_shift_value():
        """Generate a shift value based on SECRET logic"""
        return sum(ord(c) for c in CryptoUtil.SECRET) % 256  # Modulo to keep within byte range

    @staticmethod
    def encrypt_password(password):
        """Encrypt a password using a custom shift logic"""
        shift = CryptoUtil._get_shift_value()
        encrypted_bytes = bytes([b ^ shift for b in password.encode()])
        return base64.urlsafe_b64encode(encrypted_bytes).decode()

    @staticmethod
    def decrypt_password(encrypted_password):
        """Decrypt an encrypted password using the same shift logic"""
        shift = CryptoUtil._get_shift_value()
        encrypted_bytes = base64.urlsafe_b64decode(encrypted_password)
        decrypted_bytes = bytes([b ^ shift for b in encrypted_bytes])
        return decrypted_bytes.decode()

def main():
    print("=== Password Encryption Utility ===")
    plain_password = input("Enter the password to encrypt: ").strip()

    if not plain_password:
        print("Error: Password cannot be empty!")
        return

    encrypted_password = CryptoUtil.encrypt_password(plain_password)
    print("\nEncrypted Password (copy this):")
    print(encrypted_password)

if __name__ == "__main__":
    main()
