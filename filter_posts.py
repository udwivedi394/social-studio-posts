from collections import defaultdict

import tldextract

from utils import JSONUtils


def get_domain(url):
    list_2 = tldextract.extract(url)
    domain_name = list_2.domain
    return domain_name


class CombineJSONs:
    def combine_jsons(self, list_of_files):
        json_map = self._load_jsons(list_of_files)
        topic_json_map = self._club_jsons(json_map)
        return topic_json_map

    def _load_jsons(self, list_of_files):
        json_map = dict()
        for json_file in list_of_files:
            json_data = JSONUtils.load_json_data_from_file(json_file)
            json_map[json_file] = json_data
        return json_map

    def _club_jsons(self, json_map):
        topic_json_map = dict()
        for json_data in json_map.values():
            topic_ids = json_data['data'][0]['topics']
            for topic_id in topic_ids:
                if topic_id not in topic_json_map:
                    topic_json_map[topic_id] = {
                        'data': json_data['data'],
                        'meta': json_data['meta']
                    }
                else:
                    data_list = json_data['data']
                    existing_data_list = topic_json_map[topic_id]['data']
                    total_data = existing_data_list + data_list
                    topic_json_map[topic_id]['data'] = total_data,
                    topic_json_map[topic_id]['meta']['totalCount'] += json_data['meta']['totalCount']
        return topic_json_map


class FilterPosts:
    def execute(self, input_file):
        json_data = JSONUtils.load_json_data_from_file(input_file)
        posts = json_data['data']
        self._get_breakup(posts)

    def _get_breakup(self, posts):
        print('ProfileID: {}'.format(posts[0]['topics'][0]))
        self._get_sources(posts)
        self._get_domain_breakup(posts)
        types_of_posts = self._get_type_of_posts(posts)
        self._print_content_breakup(posts)
        original_posts = self._print_root_posts(posts)
        self._print_post_type_for_original_posts(original_posts)
        self._print_comments(types_of_posts)
        self._print_author_names(posts)
        self._print_author_links(posts)
        post_with_dynamics = self._print_posts_with_dynamics(types_of_posts)
        self._print_domain_dynamics(post_with_dynamics)
        self._line_break()

    def dynamics_filter(self, post):
        for dynamics in post['postDynamics']:
            if dynamics['value'] != '0':
                return True
        return False

    def _print_dict_lengths(self, post_type_dynamics, type_of_map=''):
        print('Printing the map for {}: '.format(type_of_map))
        index = ord('a')
        list_keys = list(post_type_dynamics.keys())
        list_keys.sort(key=lambda x: len(post_type_dynamics[x]), reverse=True)
        # for key, value in post_type_dynamics.items():
        #     print('\t{}. {}: {}'.format(chr(index), key, len(value)))
        #     index += 1
        for key in list_keys:
            value = post_type_dynamics[key]
            print('\t{}. {}: {}'.format(chr(index), key, len(value)))
            index += 1

    def _get_sources(self, posts):
        unique_domains = set(map(lambda x: get_domain(x['externalLink']), posts))
        social_media_domains = set(['twitter', 'facebook', 'youtube'])
        social_media_domain_list = ', '.join(social_media_domains.intersection(unique_domains))
        print('Social Media Sources: {}'.format(social_media_domain_list))
        unique_domains_list = ', '.join(unique_domains - social_media_domains)
        print('Other Sources: {}'.format(unique_domains_list))

    def _get_type_of_posts(self, posts):
        types_of_posts = defaultdict(list)
        for post in posts:
            types_of_posts[post['postType']].append(post)
        unique_type = ', '.join(map(str, types_of_posts.keys()))
        print('Type of Posts: {}'.format(unique_type))
        self._print_dict_lengths(types_of_posts, 'Post Type')
        return types_of_posts

    def _print_content_breakup(self, posts):
        posts_with_content = list(filter(lambda x: x['content'], posts))
        print('Total posts: {}, Posts with content: {}'.format(len(posts), len(posts_with_content)))

    def _print_root_posts(self, posts):
        original_posts = list(filter(lambda x: x['parent'] is None, posts))
        print('Root Posts (Parent=None): {}'.format(len(original_posts)))
        return original_posts

    def _print_comments(self, types_of_posts):
        comments = list(filter(lambda x: x['parent'], types_of_posts[None]))
        print('Comments: {}'.format(len(comments)))

    def _print_author_names(self, posts):
        posts_with_authors = list(filter(lambda x: x['author']['authorFullName'], posts))
        print('Posts with AuthorNames: {}'.format(len(posts_with_authors)))

    def _print_author_links(self, posts):
        posts_with_author_link = list(filter(lambda x: x['author']['avatar'], posts))
        print('Posts with avatar link (user image link): {}'.format(len(posts_with_author_link)))

    def _print_posts_with_dynamics(self, types_of_posts):
        post_with_dynamics = list(filter(lambda x: self.dynamics_filter(x), types_of_posts[None]))
        print('Original posts with post dynamics: {}'.format(len(post_with_dynamics)))
        return post_with_dynamics

    def _print_domain_dynamics(self, post_with_dynamics):
        post_type_dynamics = defaultdict(list)
        for post in post_with_dynamics:
            post_type_dynamics[get_domain(post['externalLink'])].append(post)
        self._print_dict_lengths(post_type_dynamics, 'Domain engagement dynamics')

    def _print_post_type_for_original_posts(self, original_posts):
        original_posts_with_post_type_ = list(filter(lambda x: x['postType'], original_posts))
        print('Original Posts with PostType other than None: {}'.format(len(original_posts_with_post_type_)))

    def _line_break(self):
        print('\n\n\n\n')

    def _get_domain_breakup(self, posts):
        domain_count_map = defaultdict(list)
        for post in posts:
            domain_count_map[get_domain(post['externalLink'])].append(post)
        self._print_dict_lengths(domain_count_map, 'Source post count')


def main():
    FilterPosts().execute('start_tv_posts/1056316/posts_1056316.json')
    FilterPosts().execute('start_tv_posts/1441134/posts_1441134.json')
    FilterPosts().execute('start_tv_posts/1446432/posts_1446432.json')


if __name__ == '__main__':
    main()
