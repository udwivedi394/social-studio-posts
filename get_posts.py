import sys
from datetime import datetime, timedelta

from request_maker import RequestMaker
from utils import JSONUtils, OSFileOperations


class EpochGenerator:
    def get_new_epoch_time(self, date=datetime.now(), days=0):
        delta = timedelta(days=days)
        new_date = date + delta
        epoch_time = new_date.timestamp()
        return int(epoch_time) * 1000


class StarTVClient:
    credentials_file = 'credentials.json'

    def __init__(self):
        self.client_id = None
        self.client_secret = None
        self.username = None
        self.password = None
        self._populate_credentials()

    def get_client_info(self):
        client_info = {
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        return client_info

    def get_user_account_info(self):
        user_account_info = {
            "username": self.username,
            "password": self.password
        }
        return user_account_info

    def _populate_credentials(self):
        data = JSONUtils.load_json_data_from_file(self.credentials_file)
        self.client_id = data['client_id']
        self.client_secret = data['client_secret']
        self.username = data['username']
        self.password = data['password']


class AccessToken:
    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.expires_in_seconds = None
        self.generated_at_time = None

    def get(self):
        return self.access_token

    def is_token_valid(self):
        if not self.access_token:
            return False
        return self._validate_time_elapsed()

    def _validate_time_elapsed(self):
        current_time = datetime.now()
        return current_time.timestamp() - self.generated_at_time.timestamp() > self.expires_in_seconds

    def set(self, token_dict):
        self.access_token = token_dict['access_token']
        self.refresh_token = token_dict['refresh_token']
        self.expires_in_seconds = token_dict['expires_in'] - 120
        self.generated_at_time = datetime.now()


class TokenGenerator:
    def __init__(self, client_info=StarTVClient()):
        self.url = 'https://api.socialstudio.radian6.com/oauth/token'
        self.client_info = client_info
        self.access_token = None

    def _generate_token(self):
        client_info = self.client_info.get_client_info()
        user_info = self.client_info.get_user_account_info()
        request_parameters = {**client_info, **user_info, "grant_type": "password"}
        response = self._make_request(request_parameters)
        return self._gen_access_token(response)

    def _refresh_access_token(self):
        client_info = self.client_info.get_client_info()
        refresh_token = {'refresh_token': self.access_token.refresh_token}
        request_parameters = {**client_info, **refresh_token, "grant_type": "refresh_token"}
        response = self._make_request(request_parameters)
        return self._gen_access_token(response)

    def _make_request(self, request_parameters):
        request_maker = RequestMaker()
        response = request_maker.post_request(url=self.url, json=request_parameters)
        return response.json()

    def get_token(self):
        if not self.access_token:
            self.access_token = self._generate_token()
        elif not self.access_token.is_token_valid():
            self.access_token = self._refresh_access_token()
        return self.access_token.get()

    def _gen_access_token(self, response):
        access_token = AccessToken()
        access_token.set(response)
        return access_token


class FileComponents:
    def __init__(self, profile_id):
        self.profile_id = profile_id
        self.file_with_content = None
        self.file_without_content = None
        self.file_with_all_posts = None
        self.state_file = None
        self._gen_names()

    def _gen_names(self):
        self.file_with_content = 'start_tv_posts_2/{profile_id}/with_content/posts_with_{profile_id}.json'.format(
            profile_id=self.profile_id)
        self.file_without_content = 'start_tv_posts_2/{profile_id}/without_content/posts_without_{profile_id}.json'.format(
            profile_id=self.profile_id)
        self.file_with_all_posts = 'start_tv_posts_2/{profile_id}/posts_{profile_id}.json'.format(
            profile_id=self.profile_id)
        self.state_file = 'start_tv_posts_2/{profile_id}/state_{profile_id}.json'.format(
            profile_id=self.profile_id)


class PostExtractor:
    def __init__(self, token_generator):
        self.token_generator = token_generator
        self.url = 'https://api.socialstudio.radian6.com/v3/posts'
        self.file_components = None

    def write_api_data(self, profile_id):
        self.file_components = FileComponents(profile_id)
        data = self.get_posts(profile_id)
        self._write_data_to_json(self.file_components.file_with_all_posts, data)

    def get_posts(self, profile_id):
        params = self._load_parameters(profile_id)
        request_maker = RequestMaker()
        pagination = True
        total_count_with_content, total_count_without_count = 0, 0
        total_data = {'data': [],
                      'meta': {
                          'totalCount': 0
                      }}
        while pagination:
            print('Making API call at {}: {}'.format(datetime.now(), params))
            header = self._get_header()
            response = request_maker.get_request(self.url, params=params, header=header)
            resp_data = response.json()
            posts_with_content, posts_without_content = self._segregate_posts(resp_data['data'])
            total_count_with_content += len(posts_with_content)
            total_count_without_count += len(posts_without_content)
            self._write_data_to_json(self.file_components.file_with_content, posts_with_content)
            self._write_data_to_json(self.file_components.file_without_content, posts_without_content)
            remaining_count = resp_data['meta']['totalCount']
            if remaining_count <= 1:
                pagination = False
            else:
                last_id = resp_data['data'][-1]['id']
                # next_params = {'beforeId': last_id}
                next_params = {'sinceId': last_id}
                params = {**params, **next_params}
            self._write_state(params)
            print('With content: {}, without content: {} at {}'.format(total_count_with_content,
                                                                       total_count_without_count,
                                                                       datetime.now()))
        return total_data

    def _get_header(self):
        access_token = self.token_generator.get_token()
        bearer_token = 'Bearer {}'.format(access_token)
        header = {'Authorization': bearer_token}
        return header

    def _write_data_to_json(self, file_name, posts):
        if OSFileOperations.entity_exists(file_name):
            total_data = JSONUtils.load_json_data_from_file(file_name)
        else:
            total_data = {'data': [],
                          'meta': {
                              'totalCount': 0
                          }}
        total_data['data'] += posts
        total_data['meta']['totalCount'] += len(posts)
        JSONUtils.write_json_data_to_file(file_name, total_data)

    def _segregate_posts(self, posts):
        with_content, without_content = [], []
        for post in posts:
            if post['content']:
                with_content.append(post)
            else:
                without_content.append(post)
        return with_content, without_content

    def _load_parameters(self, profile_id):
        if OSFileOperations.entity_exists(self.file_components.state_file):
            params = JSONUtils.load_json_data_from_file(self.file_components.state_file)
        else:
            epoch_time = EpochGenerator().get_new_epoch_time(days=-91)
            params = {'topics': profile_id, 'limit': 1000, 'startDate': epoch_time,
                      'sortBy': 'publishedDate-ascending'
                      }
        return params

    def _write_state(self, params):
        JSONUtils.write_json_data_to_file(self.file_components.state_file, params)


def main():
    profile_id = sys.argv[1]
    token_generator = TokenGenerator()
    post_extractor = PostExtractor(token_generator)
    post_extractor.write_api_data(profile_id)


if __name__ == '__main__':
    main()
