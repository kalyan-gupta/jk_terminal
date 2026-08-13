from rest_framework import serializers
from django.contrib.auth.models import User
from ..models import UserNeoCredentials, BasketOrder, TrackedOrder, ActiveMarketData

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ('id', 'username', 'email', 'first_name', 'last_name')

class RegisterSerializer(serializers.ModelSerializer):
    password = serializers.CharField(write_only=True)

    class Meta:
        model = User
        fields = ('username', 'email', 'password', 'first_name', 'last_name')

    def create(self, validated_data):
        user = User.objects.create_user(
            username=validated_data['username'],
            email=validated_data['email'],
            password=validated_data['password'],
            first_name=validated_data.get('first_name', ''),
            last_name=validated_data.get('last_name', '')
        )
        return user

class UserNeoCredentialsSerializer(serializers.ModelSerializer):
    mpin = serializers.CharField(write_only=True, required=False)
    consumer_key = serializers.CharField(write_only=True, required=False)
    mobile_number = serializers.CharField(required=False)
    totp_secret = serializers.CharField(write_only=True, required=False)

    class Meta:
        model = UserNeoCredentials
        fields = ('ucc', 'account_name', 'auth_mode', 'mpin', 'consumer_key', 'mobile_number', 'totp_secret')

    def to_representation(self, instance):
        ret = super().to_representation(instance)
        # Masks for sensitive fields on retrieval
        ret['mpin'] = '••••••••' if instance.mpin else None
        ret['consumer_key'] = '••••••••' if instance.consumer_key else None
        ret['totp_secret'] = '••••••••' if instance.totp_secret else None
        # Partially mask mobile number
        decrypted_mobile = instance.decrypt_field(instance.mobile_number)
        if decrypted_mobile and len(decrypted_mobile) > 4:
            ret['mobile_number'] = decrypted_mobile[:3] + '*' * (len(decrypted_mobile) - 7) + decrypted_mobile[-4:]
        else:
            ret['mobile_number'] = '••••••••'
        return ret

    def update(self, instance, validated_data):
        # Only update sensitive fields if they aren't the masked string
        mpin = validated_data.get('mpin')
        if mpin and mpin != '••••••••':
            instance.mpin = mpin
        
        consumer_key = validated_data.get('consumer_key')
        if consumer_key and consumer_key != '••••••••':
            instance.consumer_key = consumer_key

        mobile_number = validated_data.get('mobile_number')
        if mobile_number and mobile_number != '••••••••':
            instance.mobile_number = mobile_number

        totp_secret = validated_data.get('totp_secret')
        if totp_secret and totp_secret != '••••••••':
            instance.totp_secret = totp_secret

        instance.ucc = validated_data.get('ucc', instance.ucc)
        instance.account_name = validated_data.get('account_name', instance.account_name)
        instance.auth_mode = validated_data.get('auth_mode', instance.auth_mode)
        instance.save()
        return instance

class BasketOrderSerializer(serializers.ModelSerializer):
    class Meta:
        model = BasketOrder
        fields = '__all__'
        read_only_fields = ('user', 'created_at')

class TrackedOrderSerializer(serializers.ModelSerializer):
    class Meta:
        model = TrackedOrder
        fields = '__all__'
        read_only_fields = ('user', 'created_at', 'updated_at')

class ActiveMarketDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = ActiveMarketData
        fields = '__all__'
