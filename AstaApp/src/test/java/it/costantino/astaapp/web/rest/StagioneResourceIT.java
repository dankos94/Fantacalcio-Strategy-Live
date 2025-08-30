package it.costantino.astaapp.web.rest;

import static it.costantino.astaapp.domain.StagioneAsserts.*;
import static it.costantino.astaapp.web.rest.TestUtil.createUpdateProxyForBean;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.costantino.astaapp.IntegrationTest;
import it.costantino.astaapp.domain.Stagione;
import it.costantino.astaapp.repository.StagioneRepository;
import it.costantino.astaapp.service.dto.StagioneDTO;
import it.costantino.astaapp.service.mapper.StagioneMapper;
import jakarta.persistence.EntityManager;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

/**
 * Integration tests for the {@link StagioneResource} REST controller.
 */
@IntegrationTest
@AutoConfigureMockMvc
@WithMockUser
class StagioneResourceIT {

    private static final String DEFAULT_NOME = "AAAAAAAAAA";
    private static final String UPDATED_NOME = "BBBBBBBBBB";

    private static final String ENTITY_API_URL = "/api/stagiones";
    private static final String ENTITY_API_URL_ID = ENTITY_API_URL + "/{id}";

    private static Random random = new Random();
    private static AtomicLong longCount = new AtomicLong(random.nextInt() + (2 * Integer.MAX_VALUE));

    @Autowired
    private ObjectMapper om;

    @Autowired
    private StagioneRepository stagioneRepository;

    @Autowired
    private StagioneMapper stagioneMapper;

    @Autowired
    private EntityManager em;

    @Autowired
    private MockMvc restStagioneMockMvc;

    private Stagione stagione;

    private Stagione insertedStagione;

    /**
     * Create an entity for this test.
     *
     * This is a static method, as tests for other entities might also need it,
     * if they test an entity which requires the current entity.
     */
    public static Stagione createEntity() {
        return new Stagione().nome(DEFAULT_NOME);
    }

    /**
     * Create an updated entity for this test.
     *
     * This is a static method, as tests for other entities might also need it,
     * if they test an entity which requires the current entity.
     */
    public static Stagione createUpdatedEntity() {
        return new Stagione().nome(UPDATED_NOME);
    }

    @BeforeEach
    void initTest() {
        stagione = createEntity();
    }

    @AfterEach
    void cleanup() {
        if (insertedStagione != null) {
            stagioneRepository.delete(insertedStagione);
            insertedStagione = null;
        }
    }

    @Test
    @Transactional
    void createStagione() throws Exception {
        long databaseSizeBeforeCreate = getRepositoryCount();
        // Create the Stagione
        StagioneDTO stagioneDTO = stagioneMapper.toDto(stagione);
        var returnedStagioneDTO = om.readValue(
            restStagioneMockMvc
                .perform(
                    post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(stagioneDTO))
                )
                .andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString(),
            StagioneDTO.class
        );

        // Validate the Stagione in the database
        assertIncrementedRepositoryCount(databaseSizeBeforeCreate);
        var returnedStagione = stagioneMapper.toEntity(returnedStagioneDTO);
        assertStagioneUpdatableFieldsEquals(returnedStagione, getPersistedStagione(returnedStagione));

        insertedStagione = returnedStagione;
    }

    @Test
    @Transactional
    void createStagioneWithExistingId() throws Exception {
        // Create the Stagione with an existing ID
        stagione.setId(1L);
        StagioneDTO stagioneDTO = stagioneMapper.toDto(stagione);

        long databaseSizeBeforeCreate = getRepositoryCount();

        // An entity with an existing ID cannot be created, so this API call must fail
        restStagioneMockMvc
            .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(stagioneDTO)))
            .andExpect(status().isBadRequest());

        // Validate the Stagione in the database
        assertSameRepositoryCount(databaseSizeBeforeCreate);
    }

    @Test
    @Transactional
    void checkNomeIsRequired() throws Exception {
        long databaseSizeBeforeTest = getRepositoryCount();
        // set the field null
        stagione.setNome(null);

        // Create the Stagione, which fails.
        StagioneDTO stagioneDTO = stagioneMapper.toDto(stagione);

        restStagioneMockMvc
            .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(stagioneDTO)))
            .andExpect(status().isBadRequest());

        assertSameRepositoryCount(databaseSizeBeforeTest);
    }

    @Test
    @Transactional
    void getAllStagiones() throws Exception {
        // Initialize the database
        insertedStagione = stagioneRepository.saveAndFlush(stagione);

        // Get all the stagioneList
        restStagioneMockMvc
            .perform(get(ENTITY_API_URL + "?sort=id,desc"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$.[*].id").value(hasItem(stagione.getId().intValue())))
            .andExpect(jsonPath("$.[*].nome").value(hasItem(DEFAULT_NOME)));
    }

    @Test
    @Transactional
    void getStagione() throws Exception {
        // Initialize the database
        insertedStagione = stagioneRepository.saveAndFlush(stagione);

        // Get the stagione
        restStagioneMockMvc
            .perform(get(ENTITY_API_URL_ID, stagione.getId()))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$.id").value(stagione.getId().intValue()))
            .andExpect(jsonPath("$.nome").value(DEFAULT_NOME));
    }

    @Test
    @Transactional
    void getNonExistingStagione() throws Exception {
        // Get the stagione
        restStagioneMockMvc.perform(get(ENTITY_API_URL_ID, Long.MAX_VALUE)).andExpect(status().isNotFound());
    }

    @Test
    @Transactional
    void putExistingStagione() throws Exception {
        // Initialize the database
        insertedStagione = stagioneRepository.saveAndFlush(stagione);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the stagione
        Stagione updatedStagione = stagioneRepository.findById(stagione.getId()).orElseThrow();
        // Disconnect from session so that the updates on updatedStagione are not directly saved in db
        em.detach(updatedStagione);
        updatedStagione.nome(UPDATED_NOME);
        StagioneDTO stagioneDTO = stagioneMapper.toDto(updatedStagione);

        restStagioneMockMvc
            .perform(
                put(ENTITY_API_URL_ID, stagioneDTO.getId())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(stagioneDTO))
            )
            .andExpect(status().isOk());

        // Validate the Stagione in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertPersistedStagioneToMatchAllProperties(updatedStagione);
    }

    @Test
    @Transactional
    void putNonExistingStagione() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        stagione.setId(longCount.incrementAndGet());

        // Create the Stagione
        StagioneDTO stagioneDTO = stagioneMapper.toDto(stagione);

        // If the entity doesn't have an ID, it will throw BadRequestAlertException
        restStagioneMockMvc
            .perform(
                put(ENTITY_API_URL_ID, stagioneDTO.getId())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(stagioneDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Stagione in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void putWithIdMismatchStagione() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        stagione.setId(longCount.incrementAndGet());

        // Create the Stagione
        StagioneDTO stagioneDTO = stagioneMapper.toDto(stagione);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restStagioneMockMvc
            .perform(
                put(ENTITY_API_URL_ID, longCount.incrementAndGet())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(stagioneDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Stagione in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void putWithMissingIdPathParamStagione() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        stagione.setId(longCount.incrementAndGet());

        // Create the Stagione
        StagioneDTO stagioneDTO = stagioneMapper.toDto(stagione);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restStagioneMockMvc
            .perform(put(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(stagioneDTO)))
            .andExpect(status().isMethodNotAllowed());

        // Validate the Stagione in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void partialUpdateStagioneWithPatch() throws Exception {
        // Initialize the database
        insertedStagione = stagioneRepository.saveAndFlush(stagione);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the stagione using partial update
        Stagione partialUpdatedStagione = new Stagione();
        partialUpdatedStagione.setId(stagione.getId());

        restStagioneMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, partialUpdatedStagione.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(partialUpdatedStagione))
            )
            .andExpect(status().isOk());

        // Validate the Stagione in the database

        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertStagioneUpdatableFieldsEquals(createUpdateProxyForBean(partialUpdatedStagione, stagione), getPersistedStagione(stagione));
    }

    @Test
    @Transactional
    void fullUpdateStagioneWithPatch() throws Exception {
        // Initialize the database
        insertedStagione = stagioneRepository.saveAndFlush(stagione);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the stagione using partial update
        Stagione partialUpdatedStagione = new Stagione();
        partialUpdatedStagione.setId(stagione.getId());

        partialUpdatedStagione.nome(UPDATED_NOME);

        restStagioneMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, partialUpdatedStagione.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(partialUpdatedStagione))
            )
            .andExpect(status().isOk());

        // Validate the Stagione in the database

        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertStagioneUpdatableFieldsEquals(partialUpdatedStagione, getPersistedStagione(partialUpdatedStagione));
    }

    @Test
    @Transactional
    void patchNonExistingStagione() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        stagione.setId(longCount.incrementAndGet());

        // Create the Stagione
        StagioneDTO stagioneDTO = stagioneMapper.toDto(stagione);

        // If the entity doesn't have an ID, it will throw BadRequestAlertException
        restStagioneMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, stagioneDTO.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(stagioneDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Stagione in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void patchWithIdMismatchStagione() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        stagione.setId(longCount.incrementAndGet());

        // Create the Stagione
        StagioneDTO stagioneDTO = stagioneMapper.toDto(stagione);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restStagioneMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, longCount.incrementAndGet())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(stagioneDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Stagione in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void patchWithMissingIdPathParamStagione() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        stagione.setId(longCount.incrementAndGet());

        // Create the Stagione
        StagioneDTO stagioneDTO = stagioneMapper.toDto(stagione);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restStagioneMockMvc
            .perform(
                patch(ENTITY_API_URL).with(csrf()).contentType("application/merge-patch+json").content(om.writeValueAsBytes(stagioneDTO))
            )
            .andExpect(status().isMethodNotAllowed());

        // Validate the Stagione in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void deleteStagione() throws Exception {
        // Initialize the database
        insertedStagione = stagioneRepository.saveAndFlush(stagione);

        long databaseSizeBeforeDelete = getRepositoryCount();

        // Delete the stagione
        restStagioneMockMvc
            .perform(delete(ENTITY_API_URL_ID, stagione.getId()).with(csrf()).accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isNoContent());

        // Validate the database contains one less item
        assertDecrementedRepositoryCount(databaseSizeBeforeDelete);
    }

    protected long getRepositoryCount() {
        return stagioneRepository.count();
    }

    protected void assertIncrementedRepositoryCount(long countBefore) {
        assertThat(countBefore + 1).isEqualTo(getRepositoryCount());
    }

    protected void assertDecrementedRepositoryCount(long countBefore) {
        assertThat(countBefore - 1).isEqualTo(getRepositoryCount());
    }

    protected void assertSameRepositoryCount(long countBefore) {
        assertThat(countBefore).isEqualTo(getRepositoryCount());
    }

    protected Stagione getPersistedStagione(Stagione stagione) {
        return stagioneRepository.findById(stagione.getId()).orElseThrow();
    }

    protected void assertPersistedStagioneToMatchAllProperties(Stagione expectedStagione) {
        assertStagioneAllPropertiesEquals(expectedStagione, getPersistedStagione(expectedStagione));
    }

    protected void assertPersistedStagioneToMatchUpdatableProperties(Stagione expectedStagione) {
        assertStagioneAllUpdatablePropertiesEquals(expectedStagione, getPersistedStagione(expectedStagione));
    }
}
